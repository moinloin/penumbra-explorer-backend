use async_graphql::Union;
use crate::api::graphql::types::{Block, Transaction};

#[derive(Union)]
pub enum SearchResult {
    Block(Block),
    Transaction(Transaction),
}

#[derive(Union)]
pub enum Action {
    NotYetSupportedAction(NotYetSupportedAction),
}

#[derive(async_graphql::SimpleObject)]
pub struct NotYetSupportedAction {
    pub debug: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct IbcRelay {
    pub raw_action: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct Output {
    pub proof: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct Spend {
    pub proof: String,
}
