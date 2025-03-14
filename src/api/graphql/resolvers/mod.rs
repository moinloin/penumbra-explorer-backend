mod block;
mod transaction;
mod search;
mod stats;

use async_graphql::Object;

pub use block::{resolve_block, resolve_blocks};
pub use transaction::{resolve_transaction, resolve_transactions};
pub use search::resolve_search;
pub use stats::resolve_stats;

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

    /// --- Direct database queries ---

    /// Get a block directly from the database by height
    async fn db_block(&self, ctx: &async_graphql::Context<'_>, height: i64) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_by_height(ctx, height).await
    }

    /// Get a list of blocks directly from the database
    async fn db_blocks(&self, ctx: &async_graphql::Context<'_>, limit: Option<i64>, offset: Option<i64>) -> async_graphql::Result<Vec<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_all(ctx, limit, offset).await
    }

    /// Get the latest block directly from the database
    async fn db_latest_block(&self, ctx: &async_graphql::Context<'_>) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_latest(ctx).await
    }

    /// Get a transaction directly from the database by hash
    async fn db_transaction(&self, ctx: &async_graphql::Context<'_>, tx_hash_hex: String) -> async_graphql::Result<Option<crate::api::graphql::types::DbTransaction>> {
        crate::api::graphql::types::DbTransaction::get_by_hash(ctx, tx_hash_hex).await
    }

    /// Get a list of transactions directly from the database
    async fn db_transactions(&self, ctx: &async_graphql::Context<'_>, limit: Option<i64>, offset: Option<i64>) -> async_graphql::Result<Vec<crate::api::graphql::types::DbTransaction>> {
        crate::api::graphql::types::DbTransaction::get_all(ctx, limit, offset).await
    }

    /// Get transactions from a specific block directly from the database
    async fn db_transactions_by_block(&self, ctx: &async_graphql::Context<'_>, block_height: i64) -> async_graphql::Result<Vec<crate::api::graphql::types::DbTransaction>> {
        crate::api::graphql::types::DbTransaction::get_by_block(ctx, block_height).await
    }
}