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

async fn listen_for_transaction_count(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(1));
    let mut last_count: Option<i64> = None;

    loop {
        interval.tick().await;
        
        match get_transaction_count(&pool).await {
            Ok(count) => {
                if last_count.is_none() || last_count.unwrap() != count {
                    debug!("Transaction count changed: {}", count);
                    pubsub.publish_transaction_count(count);
                    last_count = Some(count);
                }
            }
            Err(e) => error!("Error fetching transaction count: {}", e),
        }
    }
}

async fn get_latest_block_height(pool: &Pool<Postgres>) -> Result<Option<i64>, sqlx::Error> {
    let result = sqlx::query!(
        r"SELECT height FROM blocks ORDER BY height DESC LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;
    
    Ok(result.map(|r| r.height))
}

async fn get_latest_transaction_id(pool: &Pool<Postgres>) -> Result<Option<i64>, sqlx::Error> {
    let result = sqlx::query!(
        r"SELECT id FROM transactions ORDER BY id DESC LIMIT 1"
    )
    .fetch_optional(pool)
    .await?;
    
    Ok(result.map(|r| r.id))
}