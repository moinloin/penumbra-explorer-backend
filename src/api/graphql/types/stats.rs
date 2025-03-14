use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct Stats {
    pub total_transactions_count: i32,
}