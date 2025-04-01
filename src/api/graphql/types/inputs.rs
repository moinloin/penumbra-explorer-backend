use crate::api::graphql::types::RangeDirection;
use async_graphql::InputObject;

#[derive(InputObject)]
pub struct BlockHeightRange {
    pub from: i32,
    pub to: i32,
}

#[derive(InputObject)]
pub struct LatestBlock {
    pub limit: i32,
}

#[derive(InputObject)]
pub struct BlocksSelector {
    pub latest: Option<LatestBlock>,
    pub range: Option<BlockHeightRange>,
}

#[derive(InputObject)]
pub struct LatestTransactions {
    pub limit: i32,
}

#[derive(InputObject)]
pub struct TransactionRange {
    pub from_tx_hash: String,
    pub direction: RangeDirection,
    pub limit: i32,
}

#[derive(InputObject)]
pub struct TransactionsSelector {
    pub latest: Option<LatestTransactions>,
    pub range: Option<TransactionRange>,
}
