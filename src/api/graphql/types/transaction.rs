use async_graphql::{SimpleObject, Context, Result};
use sqlx::Row;
use crate::api::graphql::context::ApiContext;

/// Direct representation of the explorer_transactions database table
#[derive(SimpleObject)]
pub struct ExplorerTransaction {
    pub tx_hash_hex: String,
    pub block_height: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub fee_amount: Option<String>,
    pub chain_id: Option<String>,
    pub raw_data_hex: Option<String>,
    pub json: Option<serde_json::Value>,
}

/// Implementation of the transaction queries
pub async fn get_transaction_by_hash(ctx: &Context<'_>, tx_hash_hex: String) -> Result<Option<ExplorerTransaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let tx_hash_bytes = match hex::decode(tx_hash_hex.trim_start_matches("0x")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };

    let row_result = sqlx::query(
        r#"
        SELECT
            tx_hash,
            block_height,
            timestamp,
            COALESCE(fee_amount::TEXT, '0') as fee_amount,
            chain_id,
            raw_data,
            raw_json
        FROM
            explorer_transactions
        WHERE
            tx_hash = $1
        "#
    )
        .bind(&tx_hash_bytes)
        .fetch_optional(db)
        .await?;

    if let Some(row) = row_result {
        let tx_hash: Vec<u8> = row.get("tx_hash");
        let raw_data: Option<Vec<u8>> = row.get("raw_data");

        Ok(Some(ExplorerTransaction {
            tx_hash_hex: hex::encode_upper(&tx_hash),
            block_height: row.get("block_height"),
            timestamp: row.get("timestamp"),
            fee_amount: row.get("fee_amount"),
            chain_id: row.get("chain_id"),
            raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
            json: row.get("raw_json"),
        }))
    } else {
        Ok(None)
    }
}

pub async fn get_transactions(ctx: &Context<'_>, limit: Option<i64>, offset: Option<i64>) -> Result<Vec<ExplorerTransaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let limit = limit.unwrap_or(10);
    let offset = offset.unwrap_or(0);

    let rows = sqlx::query(
        r#"
        SELECT
            tx_hash,
            block_height,
            timestamp,
            COALESCE(fee_amount::TEXT, '0') as fee_amount,
            chain_id,
            raw_data,
            raw_json
        FROM
            explorer_transactions
        ORDER BY
            timestamp DESC
        LIMIT $1 OFFSET $2
        "#
    )
        .bind(limit)
        .bind(offset)
        .fetch_all(db)
        .await?;

    let mut transactions = Vec::with_capacity(rows.len());

    for row in rows {
        let tx_hash: Vec<u8> = row.get("tx_hash");
        let raw_data: Option<Vec<u8>> = row.get("raw_data");

        transactions.push(ExplorerTransaction {
            tx_hash_hex: hex::encode_upper(&tx_hash),
            block_height: row.get("block_height"),
            timestamp: row.get("timestamp"),
            fee_amount: row.get("fee_amount"),
            chain_id: row.get("chain_id"),
            raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
            json: row.get("raw_json"),
        });
    }

    Ok(transactions)
}

pub async fn get_transactions_by_block(ctx: &Context<'_>, block_height: i64) -> Result<Vec<ExplorerTransaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let rows = sqlx::query(
        r#"
        SELECT
            tx_hash,
            block_height,
            timestamp,
            COALESCE(fee_amount::TEXT, '0') as fee_amount,
            chain_id,
            raw_data,
            raw_json
        FROM
            explorer_transactions
        WHERE
            block_height = $1
        ORDER BY
            timestamp ASC
        "#
    )
        .bind(block_height)
        .fetch_all(db)
        .await?;

    let mut transactions = Vec::with_capacity(rows.len());

    for row in rows {
        let tx_hash: Vec<u8> = row.get("tx_hash");
        let raw_data: Option<Vec<u8>> = row.get("raw_data");

        transactions.push(ExplorerTransaction {
            tx_hash_hex: hex::encode_upper(&tx_hash),
            block_height: row.get("block_height"),
            timestamp: row.get("timestamp"),
            fee_amount: row.get("fee_amount"),
            chain_id: row.get("chain_id"),
            raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
            json: row.get("raw_json"),
        });
    }

    Ok(transactions)
}