use async_graphql::{EmptyMutation, EmptySubscription, Schema, Object, Context, ID, Result};
use sqlx::types::chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgPool;

use crate::api::graphql::context::Context as ApiContext;

// Define the GraphQL schema type
pub type PenumbraSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

// Create a new schema instance
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(ApiContext::new(db_pool))
        .finish()
}

// GraphQL object for Block
#[derive(async_graphql::SimpleObject)]
struct Block {
    id: ID,
    height: i64,
    hash: Option<String>,
    timestamp: String,
    num_transactions: i32,
    validator: Option<String>,
    chain_id: Option<String>,
}

// GraphQL object for Transaction
#[derive(async_graphql::SimpleObject)]
struct Transaction {
    id: ID,
    hash: String,
    block_height: i64,
    timestamp: String,
    fee_amount: String,
    chain_id: Option<String>,
}

// Query root for the GraphQL schema
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Get a block by height
    async fn block(&self, ctx: &Context<'_>, height: i64) -> Result<Option<Block>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let result = sqlx::query!(
            r#"
            SELECT
                height,
                block_hash,
                timestamp,
                num_transactions,
                validator_identity_key,
                chain_id
            FROM
                explorer_block_details
            WHERE
                height = $1
            "#,
            height
        )
            .fetch_optional(db)
            .await?;

        Ok(result.map(|row| Block {
            id: ID::from(row.height.to_string()),
            height: row.height,
            hash: row.block_hash.map(|h| hex::encode_upper(h)),
            timestamp: row.timestamp.to_rfc3339(),
            num_transactions: row.num_transactions,
            validator: row.validator_identity_key,
            chain_id: row.chain_id,
        }))
    }

    /// Get a list of recent blocks
    async fn recent_blocks(&self, ctx: &Context<'_>, limit: Option<i32>) -> Result<Vec<Block>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let limit = limit.unwrap_or(10).min(100);

        let rows = sqlx::query!(
            r#"
            SELECT
                height,
                block_hash,
                timestamp,
                num_transactions,
                validator_identity_key,
                chain_id
            FROM
                explorer_block_details
            ORDER BY
                height DESC
            LIMIT $1
            "#,
            limit
        )
            .fetch_all(db)
            .await?;

        Ok(rows.into_iter().map(|row| Block {
            id: ID::from(row.height.to_string()),
            height: row.height,
            hash: row.block_hash.map(|h| hex::encode_upper(h)),
            timestamp: row.timestamp.to_rfc3339(),
            num_transactions: row.num_transactions,
            validator: row.validator_identity_key,
            chain_id: row.chain_id,
        }).collect())
    }

    /// Get a transaction by hash
    async fn transaction(&self, ctx: &Context<'_>, hash: String) -> Result<Option<Transaction>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        // Convert hash string to bytes
        let hash_bytes = match hex::decode(hash.trim_start_matches("0x")) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };

        let result = sqlx::query!(
            r#"
            SELECT
                tx_hash,
                block_height,
                timestamp,
                fee_amount,
                chain_id
            FROM
                explorer_transactions
            WHERE
                tx_hash = $1
            "#,
            hash_bytes.as_slice()
        )
            .fetch_optional(db)
            .await?;

        Ok(result.map(|row| Transaction {
            id: ID::from(hex::encode_upper(&row.tx_hash)),
            hash: hex::encode_upper(&row.tx_hash),
            block_height: row.block_height,
            timestamp: row.timestamp.to_rfc3339(),
            fee_amount: row.fee_amount.to_string(),
            chain_id: row.chain_id,
        }))
    }

    /// Get recent transactions
    async fn recent_transactions(&self, ctx: &Context<'_>, limit: Option<i32>) -> Result<Vec<Transaction>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;
        let limit = limit.unwrap_or(10).min(100);

        let rows = sqlx::query!(
            r#"
            SELECT
                tx_hash,
                block_height,
                timestamp,
                fee_amount,
                chain_id
            FROM
                explorer_transactions
            ORDER BY
                timestamp DESC
            LIMIT $1
            "#,
            limit
        )
            .fetch_all(db)
            .await?;

        Ok(rows.into_iter().map(|row| Transaction {
            id: ID::from(hex::encode_upper(&row.tx_hash)),
            hash: hex::encode_upper(&row.tx_hash),
            block_height: row.block_height,
            timestamp: row.timestamp.to_rfc3339(),
            fee_amount: row.fee_amount.to_string(),
            chain_id: row.chain_id,
        }).collect())
    }
}