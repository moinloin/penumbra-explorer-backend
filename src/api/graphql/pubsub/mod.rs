use async_graphql::Context;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

mod triggers;
pub use triggers::start;

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
        let (blocks_tx, _) = broadcast::channel(1000);
        let (transactions_tx, _) = broadcast::channel(1000);
        let (transaction_count_tx, _) = broadcast::channel(1000);
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
        match self.blocks_tx.send(height) {
            Ok(_) => debug!("Published block update: {}", height),
            Err(e) => {
                // Handle the error case
                let receiver_count = self.blocks_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for block update");
                } else {
                    warn!("Failed to publish block update: {}", e);
                }
            }
        }
    }

    pub fn publish_transaction(&self, id: i64) {
        match self.transactions_tx.send(id) {
            Ok(_) => debug!("Published transaction update: {}", id),
            Err(e) => {
                // Handle the error case
                let receiver_count = self.transactions_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for transaction update");
                } else {
                    warn!("Failed to publish transaction update: {}", e);
                }
            }
        }
    }

    pub fn publish_transaction_count(&self, count: i64) {
        match self.transaction_count_tx.send(count) {
            Ok(_) => debug!("Published transaction count update: {}", count),
            Err(e) => {
                // Handle the error case
                let receiver_count = self.transaction_count_tx.receiver_count();
                if receiver_count == 0 {
                    debug!("No receivers for transaction count update");
                } else {
                    warn!("Failed to publish transaction count update: {}", e);
                }
            }
        }
    }

    #[must_use]
    pub fn from_context<'a>(ctx: &'a Context<'_>) -> Option<&'a Self> {
        ctx.data_opt::<Self>()
    }

    pub fn start_subscriptions(&self, pool: &Pool<Postgres>) {
        info!("Starting subscription triggers");

        let pubsub_clone = self.clone();
        let pool_clone = pool.clone();

        tokio::spawn(async move {
            start(pubsub_clone, pool_clone).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::RecvError;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_pubsub_channels() {
        let pubsub = PubSub::new();

        let mut blocks_rx = pubsub.blocks_subscribe();
        let mut txs_rx = pubsub.transactions_subscribe();
        let mut count_rx = pubsub.transaction_count_subscribe();

        pubsub.publish_block(100);
        pubsub.publish_transaction(200);
        pubsub.publish_transaction_count(50);

        tokio::select! {
            result = blocks_rx.recv() => {
                assert_eq!(result.unwrap(), 100);
            }
            () = sleep(Duration::from_millis(100)) => {
                panic!("Timeout waiting for block update");
            }
        }

        tokio::select! {
            result = txs_rx.recv() => {
                assert_eq!(result.unwrap(), 200);
            }
            () = sleep(Duration::from_millis(100)) => {
                panic!("Timeout waiting for transaction update");
            }
        }

        tokio::select! {
            result = count_rx.recv() => {
                assert_eq!(result.unwrap(), 50);
            }
            () = sleep(Duration::from_millis(100)) => {
                panic!("Timeout waiting for transaction count update");
            }
        }
    }

    #[tokio::test]
    async fn test_pubsub_lagged_receiver() {
        let pubsub = PubSub::new();
        let mut rx = pubsub.blocks_subscribe();

        for i in 0..1200 {
            pubsub.publish_block(i);
        }

        let result = rx.recv().await;
        assert!(matches!(result, Err(RecvError::Lagged(_))));

        pubsub.publish_block(1500);
        let result = rx.recv().await;
        assert_eq!(result.unwrap(), 1500);
    }
}