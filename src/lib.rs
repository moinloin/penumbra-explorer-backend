pub mod api;
pub mod app_views;
pub mod db_migrations;
pub mod options;
pub mod parsing;

pub use options::ExplorerOptions;

use anyhow::{Context, Result};
use axum::{
    http::Method,
    routing::{get, post},
    Router,
};
use cometindex::Indexer;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::net::SocketAddr;
use std::time::Duration;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::app_views::explorer::Explorer as ExplorerView;

pub struct Explorer {
    options: ExplorerOptions,
}

impl Explorer {
    #[must_use]
    pub fn new(options: ExplorerOptions) -> Self {
        Self { options }
    }

    /// Starts the explorer service
    ///
    /// # Errors
    /// Returns an error if database migrations fail, if database connections can't be established,
    /// or if the server fails to start
    ///
    /// # Panics
    /// Panics if CORS origin URLs cannot be parsed
    pub async fn run(&self) -> Result<()> {
        db_migrations::run_migrations(&self.options.dest_db_url)
            .context("Failed to run database migrations")?;

        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(&self.options.dest_db_url)
            .await
            .context("Failed to connect to destination database for API")?;

        let schema = crate::api::graphql::schema::create_schema(pool.clone());

        let cors = CorsLayer::new()
            .allow_origin([
                "http://localhost:3000".parse().unwrap(),
                "https://dev.explorer.penumbra.pklabs.me".parse().unwrap(),
                "https://explorer.penumbra.pklabs.me".parse().unwrap(),
                "https://explorer.penumbra.zone".parse().unwrap(),
            ])
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers([
                "Content-Type".parse().unwrap(),
                "Authorization".parse().unwrap(),
                "Accept".parse().unwrap(),
                "Origin".parse().unwrap(),
                "X-Requested-With".parse().unwrap(),
            ])
            .allow_credentials(true);

        let api_router = Router::new()
            .route("/graphql", post(crate::api::handlers::graphql_handler))
            .route("/graphql/playground", get(crate::api::handlers::graphiql))
            .route("/graphql/ws", get(crate::api::handlers::graphql_subscription))
            .route("/health", get(crate::api::handlers::health_check))
            .with_state(schema)
            .layer(cors);

        let api_host = "0.0.0.0";
        let api_port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());

        let addr = format!("{api_host}:{api_port}")
            .parse::<SocketAddr>()
            .expect("Invalid socket address");
        info!("Starting API server on {}", addr);

        let index_options = cometindex::opt::IndexOptions {
            dst_database_url: self.options.dest_db_url.clone(),
            genesis_json: std::path::PathBuf::from(self.options.genesis_json.clone()),
            poll_ms: Duration::from_millis(self.options.polling_interval_ms),
            chain_id: Some("penumbra".to_string()),
            exit_on_catchup: false,
        };

        let indexer = Indexer::new(self.options.source_db_url.clone(), index_options)
            .with_index(Box::new(ExplorerView::new()));

        tokio::select! {
            indexer_result = indexer.run() => {
                error!("Indexer exited: {:?}", indexer_result);
                indexer_result?;
            },
            server_result = axum::Server::bind(&addr).serve(api_router) => {
                error!("API server exited: {:?}", server_result);
                server_result.map_err(|e| anyhow::anyhow!("API server error: {}", e))?;
            }
        }

        Ok(())
    }
}
