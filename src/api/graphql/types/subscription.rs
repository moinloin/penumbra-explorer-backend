use crate::api::graphql::scalars::DateTime;
use async_graphql::SimpleObject;

#[derive(SimpleObject, Clone)]
pub struct BlockUpdate {
    pub height: i64,
    pub created_at: DateTime,
    pub transactions_count: i32,
}

#[derive(SimpleObject)]
pub struct TransactionUpdate {
    pub id: i64,
    pub hash: String,
    pub raw: String,
}

#[derive(SimpleObject)]
pub struct TransactionCountUpdate {
    pub count: i64,
}
