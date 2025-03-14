use async_graphql::{Context, Result};
use crate::api::graphql::{
    types::SearchResult,
    resolvers::{
        block::resolve_block,
        transaction::resolve_transaction
    },
};

pub async fn resolve_search(ctx: &Context<'_>, slug: String) -> Result<Option<SearchResult>> {
    // Try as a block height
    if let Ok(height) = slug.parse::<i32>() {
        if let Some(block) = resolve_block(ctx, height).await? {
            return Ok(Some(SearchResult::Block(block)));
        }
    }

    // Try as a transaction hash
    if let Some(tx) = resolve_transaction(ctx, slug).await? {
        return Ok(Some(SearchResult::Transaction(tx)));
    }

    Ok(None)
}
