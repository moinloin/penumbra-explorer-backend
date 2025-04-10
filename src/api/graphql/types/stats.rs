use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct Stats {
    #[graphql(name = "totalTransactionsCount")]
    pub total_transactions_count: i64,
}
