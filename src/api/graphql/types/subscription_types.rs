use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct BlockUpdate {
    pub height: i64,
}

#[derive(SimpleObject)]
pub struct TransactionUpdate {
    pub id: i64,
}

#[derive(SimpleObject)]
pub struct TransactionCountUpdate {
    pub count: i64,
}