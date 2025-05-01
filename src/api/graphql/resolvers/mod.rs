mod block;
mod ibc;
mod search;
mod stats;
mod subscription;
mod transaction;

use async_graphql::Object;

pub use block::{get as resolve_block, resolve_blocks, resolve_blocks_collection};
pub use ibc::{
    resolve_ibc_stats, resolve_ibc_stats_by_client_id,
    resolve_ibc_channel_pairs, resolve_ibc_channel_pairs_by_client_id
}; // Updated this line
pub use search::resolve_search;
pub use stats::resolve_stats;
pub use subscription::Root as SubscriptionRoot;
pub use transaction::{resolve_transaction, resolve_transactions, resolve_transactions_collection};

/// Root query type that combines all GraphQL queries
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a block by height
    async fn block(
        &self,
        ctx: &async_graphql::Context<'_>,
        height: i32,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::Block>> {
        resolve_block(ctx, height).await
    }

    /// Get blocks by selector
    async fn blocks(
        &self,
        ctx: &async_graphql::Context<'_>,
        selector: crate::api::graphql::types::BlocksSelector,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::Block>> {
        resolve_blocks(ctx, selector).await
    }

    /// Get blocks with pagination and optional filtering
    async fn blocks_collection(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: crate::api::graphql::types::CollectionLimit,
        filter: Option<crate::api::graphql::types::BlockFilter>,
    ) -> async_graphql::Result<crate::api::graphql::types::BlockCollection> {
        resolve_blocks_collection(ctx, limit, filter).await
    }

    /// Get a transaction by hash
    async fn transaction(
        &self,
        ctx: &async_graphql::Context<'_>,
        hash: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::Transaction>> {
        resolve_transaction(ctx, hash).await
    }

    /// Get transactions by selector
    async fn transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        selector: crate::api::graphql::types::TransactionsSelector,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::Transaction>> {
        resolve_transactions(ctx, selector).await
    }

    /// Get transactions with pagination and optional filtering
    async fn transactions_collection(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: crate::api::graphql::types::CollectionLimit,
        filter: Option<crate::api::graphql::types::TransactionFilter>,
    ) -> async_graphql::Result<crate::api::graphql::types::TransactionCollection> {
        resolve_transactions_collection(ctx, limit, filter).await
    }

    /// Search for blocks or transactions
    async fn search(
        &self,
        ctx: &async_graphql::Context<'_>,
        slug: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::SearchResult>> {
        resolve_search(ctx, slug).await
    }

    /// Get blockchain statistics
    async fn stats(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<crate::api::graphql::types::Stats> {
        resolve_stats(ctx).await
    }

    /// --- Direct database queries ---
    /// Get a block directly from the database by height
    async fn db_block(
        &self,
        ctx: &async_graphql::Context<'_>,
        height: i64,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_by_height(ctx, height).await
    }

    /// Get a list of blocks directly from the database
    async fn db_blocks(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_all(ctx, limit, offset).await
    }

    /// Get the latest block directly from the database
    async fn db_latest_block(
        &self,
        ctx: &async_graphql::Context<'_>,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbBlock>> {
        crate::api::graphql::types::DbBlock::get_latest(ctx).await
    }

    /// Get raw transaction data directly from the database by hash
    async fn db_raw_transaction(
        &self,
        ctx: &async_graphql::Context<'_>,
        tx_hash_hex: String,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::DbRawTransaction>> {
        crate::api::graphql::types::DbRawTransaction::get_by_hash(ctx, tx_hash_hex).await
    }

    /// Get raw transaction data directly from the database
    async fn db_raw_transactions(
        &self,
        ctx: &async_graphql::Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::DbRawTransaction>> {
        crate::api::graphql::types::DbRawTransaction::get_all(ctx, limit, offset, None).await
    }

    /// Get IBC stats with optional filtering
    async fn ibc_stats(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: Option<String>,
        time_period: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::IbcStats>> {
        resolve_ibc_stats(ctx, client_id, time_period, limit, offset).await
    }

    /// Get IBC stats by client ID
    async fn ibc_stats_by_client_id(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: String,
        time_period: Option<String>,
    ) -> async_graphql::Result<Option<crate::api::graphql::types::IbcStats>> {
        resolve_ibc_stats_by_client_id(ctx, client_id, time_period).await
    }

    /// Get IBC channel pairs with optional filtering
    async fn ibc_channel_pairs(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::ibc::ChannelPair>> {
        resolve_ibc_channel_pairs(ctx, client_id, limit, offset).await
    }

    /// Get IBC channel pairs for a specific client ID
    async fn ibc_channel_pairs_by_client_id(
        &self,
        ctx: &async_graphql::Context<'_>,
        client_id: String,
    ) -> async_graphql::Result<Vec<crate::api::graphql::types::ibc::ChannelPair>> {
        resolve_ibc_channel_pairs_by_client_id(ctx, client_id).await
    }
}