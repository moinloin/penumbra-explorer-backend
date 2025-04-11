use async_graphql::Context;
use sqlx::{Pool, Postgres};
use tokio::sync::broadcast;

pub mod triggers;

pub struct PubSub {
    blocks_tx: broadcast::Sender<i64>,
    transactions_tx: broadcast::Sender<i64>,
    transaction_count_tx: broadcast::Sender<i64>,
}