use crate::api::graphql::{
    context::ApiContext,
    types::{Block, BlocksSelector},
};
use async_graphql::Result;
use sqlx::Row;

/// Resolves a block by its height
///
/// # Errors
/// Returns an error if the database query fails
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_block(ctx: &async_graphql::Context<'_>, height: i32) -> Result<Option<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;
    let row = sqlx::query(
        r"
        SELECT
            height,
            timestamp,
            raw_json
        FROM
            explorer_block_details
        WHERE
            height = $1
        ",
    )
    .bind(i64::from(height))
    .fetch_optional(db)
    .await?;
    Ok(row.map(|r| {
        Block::new(
            i32::try_from(r.get::<i64, _>("height")).unwrap_or_default(),
            r.get("timestamp"),
            r.get("raw_json"),
        )
    }))
}

/// Resolves multiple blocks based on the provided selector
///
/// # Errors
/// Returns an error if the database query fails
pub async fn resolve_blocks(
    ctx: &async_graphql::Context<'_>,
    selector: BlocksSelector,
) -> Result<Vec<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;
    let (query, _params) = build_blocks_query(&selector);
    let mut query_builder = sqlx::query(&query);
    if let Some(range) = &selector.range {
        query_builder = query_builder
            .bind(i64::from(range.from))
            .bind(i64::from(range.to));
    } else if let Some(latest) = &selector.latest {
        query_builder = query_builder.bind(i64::from(latest.limit));
    }
    let rows = query_builder.fetch_all(db).await?;
    let blocks = rows
        .into_iter()
        .map(|row| {
            Block::new(
                i32::try_from(row.get::<i64, _>("height")).unwrap_or_default(),
                row.get("timestamp"),
                row.get("raw_json"),
            )
        })
        .collect();
    Ok(blocks)
}

fn build_blocks_query(selector: &BlocksSelector) -> (String, usize) {
    let mut query = String::from("SELECT height, timestamp, raw_json FROM explorer_block_details");
    let param_count;
    if let Some(_range) = &selector.range {
        query.push_str(" WHERE height BETWEEN $1 AND $2 ORDER BY height DESC");
        param_count = 2;
    } else if let Some(_latest) = &selector.latest {
        query.push_str(" ORDER BY height DESC LIMIT $1");
        param_count = 1;
    } else {
        query.push_str(" ORDER BY height DESC LIMIT 10");
        param_count = 0;
    }
    (query, param_count)
}
