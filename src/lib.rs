pub mod api;
pub mod app_views;
pub mod coordination;
pub mod db_migrations;
pub mod options;
pub mod parsing;

pub use options::ExplorerOptions;

use anyhow::{Context, Result};
use axum::{
    extract::Extension,
    routing::{get, post},
    Router, Server,
};
use cometindex::{opt::Options as CometOptions, Indexer};
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
        db_migrations::run_migrations(&self.options.dest_db_url)
            .context("Failed to run database migrations")?;

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
}
