use sqlx::{Pool, Postgres};
use tokio::time::{interval, Duration};
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

async fn listen_for_blocks(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(1));
    let mut last_height: Option<i64> = None;

    loop {
        interval.tick().await;
        
        match get_latest_block_height(&pool).await {
            Ok(Some(height)) => {
                if last_height.is_none() || last_height.unwrap() < height {
                    debug!("New block detected: {}", height);
                    pubsub.publish_block(height);
                    last_height = Some(height);
                }
            }
            Ok(None) => {},
            Err(e) => error!("Error fetching latest block: {}", e),
        }
    }
}

async fn listen_for_transactions(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(1));
    let mut last_transaction_id: Option<i64> = None;

    loop {
        interval.tick().await;
        
        match get_latest_transaction_id(&pool).await {
            Ok(Some(id)) => {
                if last_transaction_id.is_none() || last_transaction_id.unwrap() < id {
                    debug!("New transaction detected: {}", id);
                    pubsub.publish_transaction(id);
                    last_transaction_id = Some(id);
                }
            }
            Ok(None) => {},
            Err(e) => error!("Error fetching latest transaction: {}", e),
        }
    }
}