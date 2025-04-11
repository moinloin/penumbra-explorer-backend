use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct BlockUpdate {
    pub height: i64,
}