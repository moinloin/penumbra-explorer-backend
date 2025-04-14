// src/api/graphql/types/subscription_types.rs
use async_graphql::SimpleObject;
use crate::api::graphql::scalars::DateTime;

#[derive(SimpleObject)]
pub struct BlockUpdate {
    pub height: i64,
    pub created_at: DateTime,
    pub transactions_count: i32,
}

#[derive(SimpleObject)]
pub struct TransactionUpdate {
    pub id: i64,          // This is block_height now
    pub hash: String,     // Transaction hash as hex
    pub raw: String,      // Raw transaction data
}

#[derive(SimpleObject)]
pub struct TransactionCountUpdate {
    pub count: i64,
}