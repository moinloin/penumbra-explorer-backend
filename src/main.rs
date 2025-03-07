use anyhow::Result;
use clap::Parser;
use penumbra_explorer::{Explorer, ExplorerOptions};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse command line arguments
    let opts = ExplorerOptions::parse();

    // Create and run the explorer
    let explorer = Explorer::new(opts);
    explorer.run().await?;

    Ok(())
}