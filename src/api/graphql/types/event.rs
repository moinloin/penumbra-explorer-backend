use async_graphql::SimpleObject;

#[derive(SimpleObject)]
pub struct Event {
    #[graphql(name = "type")]
    pub type_: String,
    pub value: String,
}
