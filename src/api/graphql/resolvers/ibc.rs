use crate::api::graphql::types::ibc::Stats;
use async_graphql::{Context, Result};

/// Resolves IBC stats with optional filtering
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_stats(
    ctx: &Context<'_>,
    client_id: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<Stats>> {
    Stats::get_all(ctx, client_id, limit, offset).await
}

/// Resolves an IBC stats entry by `client_id`
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_ibc_stats_by_client_id(
    ctx: &Context<'_>,
    client_id: String,
) -> Result<Option<Stats>> {
    Stats::get_by_client_id(ctx, client_id).await
}