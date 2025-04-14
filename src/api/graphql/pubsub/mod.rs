use async_graphql::Context;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

#[derive(Clone)]
pub struct PubSub {
    blocks_tx: broadcast::Sender<i64>,
    transactions_tx: broadcast::Sender<i64>,
    transaction_count_tx: broadcast::Sender<i64>,
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSub {
    #[must_use]
    pub fn new() -> Self {
        let (blocks_tx, _) = broadcast::channel(100);
        let (transactions_tx, _) = broadcast::channel(100);
        let (transaction_count_tx, _) = broadcast::channel(100);
        Self {
            blocks_tx,
            transactions_tx,
            transaction_count_tx,
        }
    }

    #[must_use]
    pub fn blocks_subscribe(&self) -> broadcast::Receiver<i64> {
        self.blocks_tx.subscribe()
    }

    #[must_use]
    pub fn transactions_subscribe(&self) -> broadcast::Receiver<i64> {
        self.transactions_tx.subscribe()
    }

    #[must_use]
    pub fn transaction_count_subscribe(&self) -> broadcast::Receiver<i64> {
        self.transaction_count_tx.subscribe()
    }

    pub fn publish_block(&self, height: i64) {
        let _ = self.blocks_tx.send(height);
    }

    pub fn publish_transaction(&self, id: i64) {
        let _ = self.transactions_tx.send(id);
    }

    pub fn publish_transaction_count(&self, count: i64) {
        let _ = self.transaction_count_tx.send(count);
    }

    #[must_use]
    pub fn from_context<'a>(ctx: &'a Context<'_>) -> Option<&'a Self> {
        ctx.data_opt::<Self>()
    }

    pub fn start_triggers(&self, pool: &Pool<Postgres>) {
        info!("Starting subscription triggers");

        let pubsub_blocks = self.clone();
        let pool_blocks = pool.clone();
        tokio::spawn(async move {
            listen_for_blocks(pubsub_blocks, pool_blocks.clone()).await;
        });

        let pubsub_txs = self.clone();
        let pool_txs = pool.clone();
        tokio::spawn(async move {
            listen_for_transactions(pubsub_txs, pool_txs.clone()).await;
        });

        let pubsub_count = self.clone();
        let pool_count = pool.clone();
        tokio::spawn(async move {
            listen_for_transaction_count(pubsub_count, pool_count.clone()).await;
        });
    }
}

async fn listen_for_blocks(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(6));
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
            Ok(None) => {}
            Err(e) => error!("Error fetching latest block: {}", e),
        }
    }
}

async fn listen_for_transactions(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(6));
    let mut last_tx_height: Option<i64> = None;
    loop {
        interval.tick().await;

        match get_latest_transaction_height(&pool).await {
            Ok(Some(height)) => {
                if last_tx_height.is_none() || last_tx_height.unwrap() < height {
                    debug!("New transaction detected at height: {}", height);
                    pubsub.publish_transaction(height);
                    last_tx_height = Some(height);
                }
            }
            Ok(None) => {}
            Err(e) => error!("Error fetching latest transaction: {}", e),
        }
    }
}

async fn listen_for_transaction_count(pubsub: PubSub, pool: Pool<Postgres>) {
    let mut interval = interval(Duration::from_secs(6));
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
    let result = sqlx::query_as::<_, (i64,)>(
        "SELECT height FROM explorer_block_details ORDER BY height DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(result.map(|r| r.0))
}

async fn get_latest_transaction_height(pool: &Pool<Postgres>) -> Result<Option<i64>, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64,)>(
        "SELECT block_height FROM explorer_transactions ORDER BY timestamp DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await?;
    Ok(result.map(|r| r.0))
}

async fn get_transaction_count(pool: &Pool<Postgres>) -> Result<i64, sqlx::Error> {
    let result = sqlx::query_as::<_, (i64,)>("SELECT COUNT(*) FROM explorer_transactions")
        .fetch_one(pool)
        .await?;
    Ok(result.0)
}
