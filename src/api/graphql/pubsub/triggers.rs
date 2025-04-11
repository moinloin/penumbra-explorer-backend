use sqlx::{Pool, Postgres};
use tracing::{debug, error, info};

use super::PubSub;

pub async fn start_triggers(pubsub: PubSub, pool: Pool<Postgres>) {
    info!("Starting subscription triggers");
    
    tokio::join!(
        listen_for_blocks(pubsub.clone(), pool.clone()),
        listen_for_transactions(pubsub.clone(), pool.clone()),
        listen_for_transaction_count(pubsub, pool)
    );
}