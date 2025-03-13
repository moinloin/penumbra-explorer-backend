mod block;
mod transaction;
mod search;
mod stats;

use async_graphql::Object;

// Import all resolvers
use block::{resolve_block, resolve_blocks};
use transaction::{resolve_transaction, resolve_transactions};
use search::resolve_search;
use stats::resolve_stats;

/// Root query type that combines all GraphQL queries
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a block by height
    async fn block(&self, ctx: &async_graphql::Context<'_>, height: i32) -> async_graphql::Result<Option<crate::api::graphql::types::Block>> {
        resolve_block(ctx, height).await
    }

    /// Get blocks by selector
    async fn blocks(&self, ctx: &async_graphql::Context<'_>, selector: crate::api::graphql::types::BlocksSelector) -> async_graphql::Result<Vec<crate::api::graphql::types::Block>> {
        resolve_blocks(ctx, selector).await
    }

    /// Get a transaction by hash
    async fn transaction(&self, ctx: &async_graphql::Context<'_>, hash: String) -> async_graphql::Result<Option<crate::api::graphql::types::Transaction>> {
        resolve_transaction(ctx, hash).await
    }

    /// Get transactions by selector
    async fn transactions(&self, ctx: &async_graphql::Context<'_>, selector: crate::api::graphql::types::TransactionsSelector) -> async_graphql::Result<Vec<crate::api::graphql::types::Transaction>> {
        resolve_transactions(ctx, selector).await
    }

    /// Search for blocks or transactions
    async fn search(&self, ctx: &async_graphql::Context<'_>, slug: String) -> async_graphql::Result<Option<crate::api::graphql::types::SearchResult>> {
        resolve_search(ctx, slug).await
    }

    /// Get blockchain statistics
    async fn stats(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<crate::api::graphql::types::Stats> {
        resolve_stats(ctx).await
    }
}