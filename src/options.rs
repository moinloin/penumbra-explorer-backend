use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Clone, Debug)]
#[command(version, about = "Penumbra Explorer Backend")]
pub struct ExplorerOptions {
    /// The database URL for the source raw events
    #[arg(short = 's', long, env = "PENUMBRA_EXPLORER_SOURCE_DB_URL")]
    pub source_db_url: String,

    /// The database URL for the destination compiled events
    #[arg(short = 'd', long, env = "PENUMBRA_EXPLORER_DEST_DB_URL")]
    pub dest_db_url: String,

    /// The genesis JSON file path
    #[arg(long, env = "PENUMBRA_EXPLORER_GENESIS_JSON")]
    pub genesis_json: String,

    /// The height to start processing from (inclusive)
    #[arg(long, env = "PENUMBRA_EXPLORER_FROM_HEIGHT")]
    pub from_height: Option<u64>,

    /// The height to process until (inclusive)
    #[arg(long, env = "PENUMBRA_EXPLORER_TO_HEIGHT")]
    pub to_height: Option<u64>,

    /// The number of blocks to process in a batch
    #[arg(long, default_value = "100", env = "PENUMBRA_EXPLORER_BATCH_SIZE")]
    pub batch_size: u64,

    /// The interval in milliseconds to poll for new blocks
    #[arg(long, default_value = "1000", env = "PENUMBRA_EXPLORER_POLLING_INTERVAL_MS")]
    pub polling_interval_ms: u64,
}