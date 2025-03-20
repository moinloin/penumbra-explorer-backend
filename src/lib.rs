pub mod api;
pub mod app_views;
pub mod coordination;
pub mod options;
pub mod parsing;

pub use options::ExplorerOptions;

use anyhow::{Context, Result};
use axum::{
    extract::Extension,
    routing::{get, post},
    Router, Server,
};
use cometindex::{opt::Options as CometOptions, Indexer, PgTransaction};
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::app_views::{block_details::BlockDetails, transactions::Transactions};
use crate::coordination::TransactionQueue;

pub struct Explorer {
    options: ExplorerOptions,
}

impl Explorer {
    pub fn new(options: ExplorerOptions) -> Self {
        Self { options }
    }

    pub async fn run(&self) -> Result<()> {
        self.init_database().await?;

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&self.options.dest_db_url)
            .await
            .context("Failed to connect to destination database for API")?;

        let schema = crate::api::graphql::schema::create_schema(pool.clone());

        let api_router = Router::new()
            .route("/graphql", post(crate::api::handlers::graphql_handler))
            .route(
                "/graphql/playground",
                get(crate::api::handlers::graphql_playground),
            )
            .route("/health", get(crate::api::handlers::health_check))
            .layer(Extension(schema));

        let api_host = "0.0.0.0";
        let api_port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());

        let addr = format!("{}:{}", api_host, api_port)
            .parse::<SocketAddr>()
            .expect("Invalid socket address");
        info!("Starting API server on {}", addr);

        let comet_options = CometOptions {
            src_database_url: self.options.source_db_url.clone(),
            dst_database_url: self.options.dest_db_url.clone(),
            genesis_json: self.options.genesis_json.clone().into(),
            poll_ms: Duration::from_millis(self.options.polling_interval_ms),
            chain_id: Some("penumbra".to_string()),
            exit_on_catchup: false,
        };

        let transaction_queue = Arc::new(Mutex::new(TransactionQueue::new()));

        let indexer = Indexer::new(comet_options)
            .with_index(Box::new(BlockDetails::new(transaction_queue.clone())))
            .with_index(Box::new(Transactions::new(transaction_queue)));

        tokio::select! {
            indexer_result = indexer.run() => {
                error!("Indexer exited: {:?}", indexer_result);
                indexer_result?;
            },
            server_result = Server::bind(&addr).serve(api_router.into_make_service()) => {
                error!("API server exited: {:?}", server_result);
                server_result.map_err(|e| anyhow::anyhow!("API server error: {}", e))?;
            }
        }

        Ok(())
    }

    async fn init_database(&self) -> Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.options.dest_db_url)
            .await
            .context("Failed to connect to destination database")?;

        let mut tx = pool.begin().await?;

        self.create_schema(&mut tx).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn create_schema(&self, tx: &mut PgTransaction<'_>) -> Result<()> {
        sqlx::query(
            r#"
    CREATE TABLE IF NOT EXISTS explorer_block_details (
        height BIGINT PRIMARY KEY,
        root BYTEA NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        num_transactions INT NOT NULL DEFAULT 0,
        total_fees NUMERIC(39, 0) DEFAULT 0,
        validator_identity_key TEXT,
        previous_block_hash BYTEA,
        block_hash BYTEA,
        chain_id TEXT,
        raw_json JSONB
    )
    "#,
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp ON explorer_block_details(timestamp DESC)")
            .execute(tx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator ON explorer_block_details(validator_identity_key)")
            .execute(tx.as_mut())
            .await?;

        sqlx::query(
            r#"
    CREATE TABLE IF NOT EXISTS explorer_transactions (
        tx_hash BYTEA PRIMARY KEY,
        block_height BIGINT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        fee_amount NUMERIC(39, 0) DEFAULT 0,
        chain_id TEXT,
        raw_data BYTEA,
        raw_json JSONB,
        FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
    )
    "#,
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height ON explorer_transactions(block_height)")
            .execute(tx.as_mut())
            .await?;

        sqlx::query("CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp ON explorer_transactions(timestamp DESC)")
            .execute(tx.as_mut())
            .await?;

        sqlx::query(
            r#"
    CREATE OR REPLACE VIEW explorer_recent_blocks AS
    SELECT
        height,
        timestamp,
        num_transactions,
        total_fees,
        validator_identity_key,
        chain_id,
        raw_json
    FROM
        explorer_block_details
    ORDER BY
        height DESC
    "#,
        )
        .execute(tx.as_mut())
        .await?;

        sqlx::query(
            r#"
    CREATE OR REPLACE VIEW explorer_transaction_summary AS
    SELECT
        t.tx_hash,
        t.block_height,
        t.timestamp,
        t.fee_amount,
        t.chain_id,
        t.raw_json
    FROM
        explorer_transactions t
    ORDER BY
        t.timestamp DESC
    "#,
        )
        .execute(tx.as_mut())
        .await?;

        Ok(())
    }
}
