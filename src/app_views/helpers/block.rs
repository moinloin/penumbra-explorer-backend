use anyhow::Result;
use serde_json::{json, Value};
use sqlx::types::chrono::DateTime;
use sqlx::{postgres::PgPool, PgTransaction};
use std::collections::HashMap;
use std::sync::Arc;

use crate::parsing;

pub struct BlockMetadata<'a> {
    pub height: u64,
    pub root: Vec<u8>,
    pub timestamp: DateTime<sqlx::types::chrono::Utc>,
    pub tx_count: usize,
    pub chain_id: &'a str,
    pub raw_json: String,
}

pub async fn fetch_chain_ids_for_blocks(
    source_pool: &Option<Arc<PgPool>>,
    heights: &[u64],
) -> Result<HashMap<u64, Option<String>>, anyhow::Error> {
    let mut result = HashMap::with_capacity(heights.len());

    if let Some(pool) = source_pool {
        let height_i64s: Vec<i64> = heights
            .iter()
            .filter_map(|&h| i64::try_from(h).ok())
            .collect();

        if height_i64s.is_empty() {
            return Ok(result);
        }

        let rows = sqlx::query_as::<_, (i64, Option<String>)>(
            "SELECT height, chain_id FROM blocks WHERE height = ANY($1)",
        )
        .bind(&height_i64s)
        .fetch_all(pool.as_ref())
        .await?;

        for (height, chain_id) in rows {
            result.insert(u64::try_from(height)?, chain_id);
        }

        for &height in heights {
            if !result.contains_key(&height) {
                result.insert(height, None);
            }
        }
    } else {
        for &height in heights {
            result.insert(height, None);
        }
    }

    Ok(result)
}

pub fn create_block_json(
    height: u64,
    chain_id: &str,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    transactions: &[Value],
    events: &[Value],
) -> String {
    let mut block_json = serde_json::Map::new();

    block_json.insert("height".to_string(), json!(height));
    block_json.insert("chain_id".to_string(), json!(chain_id));
    block_json.insert("timestamp".to_string(), json!(timestamp.to_rfc3339()));

    let txs_value = serde_json::Value::Array(transactions.to_vec());
    block_json.insert("transactions".to_string(), txs_value);

    let events_value = serde_json::Value::Array(events.to_vec());
    block_json.insert("events".to_string(), events_value);

    let json_value = serde_json::Value::Object(block_json);
    serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| "{}".to_string())
}

pub async fn insert_block(
    dbtx: &mut PgTransaction<'_>,
    meta: BlockMetadata<'_>,
) -> Result<(), anyhow::Error> {
    let height_i64 = match i64::try_from(meta.height) {
        Ok(h) => h,
        Err(e) => return Err(anyhow::anyhow!("Height conversion error: {}", e)),
    };

    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM explorer_block_details WHERE height = $1)",
    )
    .bind(height_i64)
    .fetch_one(dbtx.as_mut())
    .await?;

    let validator_key = None::<String>;
    let previous_hash = None::<Vec<u8>>;
    let block_hash = None::<Vec<u8>>;

    if exists {
        sqlx::query(
            r"
        UPDATE explorer_block_details
        SET
            root = $2,
            timestamp = $3,
            num_transactions = $4,
            chain_id = $5,
            raw_json = $6
        WHERE height = $1
        ",
        )
        .bind(height_i64)
        .bind(&meta.root)
        .bind(meta.timestamp)
        .bind(i32::try_from(meta.tx_count).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(&meta.raw_json)
        .execute(dbtx.as_mut())
        .await?;

        tracing::debug!("Updated block {}", meta.height);
    } else {
        sqlx::query(
            r"
        INSERT INTO explorer_block_details
        (height, root, timestamp, num_transactions, chain_id,
         validator_identity_key, previous_block_hash, block_hash, raw_json)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ",
        )
        .bind(height_i64)
        .bind(&meta.root)
        .bind(meta.timestamp)
        .bind(i32::try_from(meta.tx_count).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(validator_key)
        .bind(previous_hash)
        .bind(block_hash)
        .bind(&meta.raw_json)
        .execute(dbtx.as_mut())
        .await?;

        tracing::debug!("Inserted block {}", meta.height);
    }

    Ok(())
}
