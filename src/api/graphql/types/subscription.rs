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

#[derive(SimpleObject)]
#[graphql(rename_fields = "camelCase")]
pub struct IbcTransactionUpdate {
    /// Transaction hash in hex format
    pub tx_hash: String,
    /// IBC client ID associated with the transaction
    pub client_id: String,
    /// Current transaction status (pending, completed, expired, error)
    pub status: String,
    /// Block height where the transaction was included
    pub block_height: i64,
    /// Timestamp when the transaction was processed
    pub timestamp: DateTime,
    /// Flag indicating if this is a status update to an existing transaction
    pub is_status_update: bool,
    /// Raw transaction data
    pub raw: String,
}