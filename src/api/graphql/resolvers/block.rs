use async_graphql::{Context, Result};
use sqlx::Row;
use crate::api::graphql::{
    context::ApiContext,
    types::{Block, BlocksSelector},
};

pub async fn resolve_block(ctx: &Context<'_>, height: i32) -> Result<Option<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let row = sqlx::query(
        r#"
        SELECT
            height,
            timestamp,
            raw_json
        FROM
            explorer_block_details
        WHERE
            height = $1
        "#
    )
        .bind(height as i64)
        .fetch_optional(db)
        .await?;

    Ok(row.map(|r| Block::new(
        r.get::<i64, _>("height") as i32,
        r.get("timestamp"),
        r.get("raw_json"),
    )))
}

pub async fn resolve_blocks(ctx: &Context<'_>, selector: BlocksSelector) -> Result<Vec<Block>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let (query, _params) = build_blocks_query(&selector);

    let mut query_builder = sqlx::query(&query);

    if let Some(range) = &selector.range {
        query_builder = query_builder.bind(range.from as i64).bind(range.to as i64);
    } else if let Some(latest) = &selector.latest {
        query_builder = query_builder.bind(latest.limit as i64);
    }

    let rows = query_builder.fetch_all(db).await?;

    let blocks = rows.into_iter()
        .map(|row| Block::new(
            row.get::<i64, _>("height") as i32,
            row.get("timestamp"),
            row.get("raw_json"),
        ))
        .collect();

    Ok(blocks)
}

// Helper to build the SQL query based on the selector
fn build_blocks_query(selector: &BlocksSelector) -> (String, usize) {
    let mut query = String::from(
        "SELECT height, timestamp, raw_json FROM explorer_block_details"
    );
    let param_count;

    if let Some(_range) = &selector.range {
        query.push_str(" WHERE height BETWEEN $1 AND $2 ORDER BY height DESC");
        param_count = 2;
    } else if let Some(_latest) = &selector.latest {
        query.push_str(" ORDER BY height DESC LIMIT $1");
        param_count = 1;
    } else {
        // Default to latest 10 blocks
        query.push_str(" ORDER BY height DESC LIMIT 10");
        param_count = 0;
    }

    (query, param_count)
}