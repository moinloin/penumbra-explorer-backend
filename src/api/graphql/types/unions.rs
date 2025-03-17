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
    IbcRelay(IbcRelay),
    Output(Output),
    Spend(Spend),
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
    pub body: OutputBody,
    pub proof: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct OutputBody {
    pub balance_commitment: String,
    pub note_payload: NotePayload,
    pub ovk_wrapped_key: String,
    pub wrapped_memo_key: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct NotePayload {
    pub encrypted_note: String,
    pub ephemeral_key: String,
    pub note_commitment: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct Spend {
    pub auth_sig: String,
    pub body: SpendBody,
    pub proof: String,
}

#[derive(async_graphql::SimpleObject)]
pub struct SpendBody {
    pub balance_commitment: String,
    pub nullifier: String,
    pub rk: String,
}