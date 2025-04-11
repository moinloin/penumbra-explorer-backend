use async_graphql::Context;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;

pub mod triggers;

pub struct PubSub {
    blocks_tx: broadcast::Sender<i64>,
    transactions_tx: broadcast::Sender<i64>,
    transaction_count_tx: broadcast::Sender<i64>,
}

impl PubSub {
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
    
    pub fn blocks_subscribe(&self) -> broadcast::Receiver<i64> {
        self.blocks_tx.subscribe()
    }

    pub fn transactions_subscribe(&self) -> broadcast::Receiver<i64> {
        self.transactions_tx.subscribe()
    }

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
    
    pub fn from_context(ctx: &Context<'_>) -> Option<&Self> {
        ctx.data_opt::<Self>()
    }
    
    pub async fn start_triggers(self, pool: Pool<Postgres>) {
        tokio::spawn(async move {
            triggers::start_triggers(self, pool).await;
        });
    }
}