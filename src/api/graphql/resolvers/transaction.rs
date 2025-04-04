use crate::api::graphql::{
    context::ApiContext,
    types::{Block, Event, RangeDirection, Transaction, TransactionsSelector},
};
use async_graphql::Result;
use sqlx::Row;

/// Resolves a transaction by its hash
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_transaction(
    ctx: &async_graphql::Context<'_>,
    hash: String,
) -> Result<Option<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let Ok(hash_bytes) = hex::decode(hash.trim_start_matches("0x")) else {
        return Ok(None);
    };

    let row = sqlx::query(
        r"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str,
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
        WHERE
            t.tx_hash = $1
        ",
    )
    .bind(hash_bytes.as_slice())
    .fetch_optional(db)
    .await?;

    if let Some(r) = row {
        let tx_hash: Vec<u8> = r.get("tx_hash");
        let block_height: i64 = r.get("block_height");
        let timestamp: chrono::DateTime<chrono::Utc> = r.get("block_timestamp");
        let _fee_amount_str: String = r.get("fee_amount_str");
        let _chain_id: Option<String> = r.get("chain_id");
        let raw_data: String = r.get("raw_data");
        let raw_json: Option<serde_json::Value> = r.get("raw_json");

        if let Some(json) = raw_json {
            let hash = hex::encode_upper(&tx_hash);

            Ok(Some(Transaction {
                hash,
                anchor: String::new(),
                binding_sig: String::new(),
                index: extract_index_from_json(&json).unwrap_or(0),
                raw: raw_data.clone(),
                block: Block::new(
                    i32::try_from(block_height).unwrap_or_default(),
                    timestamp,
                    None,
                ),
                body: crate::api::graphql::types::extract_transaction_body(&json),
                raw_events: extract_events_from_json(&json),
                raw_json: json,
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// Resolves transactions based on the provided selector
///
/// # Errors
/// Returns an error if database queries fail
#[allow(clippy::module_name_repetitions)]
pub async fn resolve_transactions(
    ctx: &async_graphql::Context<'_>,
    selector: TransactionsSelector,
) -> Result<Vec<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let base_query = r"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str,
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
    ";

    let (query, _param_count) = build_transactions_query(&selector, base_query);

    let rows = if let Some(range) = &selector.range {
        let Ok(hash_bytes) = hex::decode(range.from_tx_hash.trim_start_matches("0x")) else {
            return Ok(vec![]);
        };

        sqlx::query(&query)
            .bind(&hash_bytes)
            .bind(i64::from(range.limit))
            .fetch_all(db)
            .await?
    } else if let Some(latest) = &selector.latest {
        sqlx::query(&query)
            .bind(i64::from(latest.limit))
            .fetch_all(db)
            .await?
    } else {
        sqlx::query(&query).fetch_all(db).await?
    };

    let mut transactions = process_transaction_rows(rows)?;

    if let Some(range) = &selector.range {
        if range.direction == RangeDirection::Previous {
            transactions.reverse();
        }
    }

    Ok(transactions)
}

#[allow(clippy::unnecessary_wraps)]
fn process_transaction_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Transaction>> {
    let mut transactions = Vec::with_capacity(rows.len());

    for row in rows {
        let tx_hash: Vec<u8> = row.get("tx_hash");
        let block_height: i64 = row.get("block_height");
        let timestamp: chrono::DateTime<chrono::Utc> = row.get("block_timestamp");
        let raw_data: String = row.get("raw_data");
        let raw_json: Option<serde_json::Value> = row.get("raw_json");

        if let Some(json) = raw_json {
            let hash = hex::encode_upper(&tx_hash);

            transactions.push(Transaction {
                hash,
                anchor: String::new(),
                binding_sig: String::new(),
                index: extract_index_from_json(&json).unwrap_or(0),
                raw: raw_data.clone(),
                block: Block::new(
                    i32::try_from(block_height).unwrap_or_default(),
                    timestamp,
                    None,
                ),
                body: crate::api::graphql::types::extract_transaction_body(&json),
                raw_events: extract_events_from_json(&json),
                raw_json: json,
            });
        }
    }

    Ok(transactions)
}

fn extract_index_from_json(json: &serde_json::Value) -> Option<i32> {
    json.get("index")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
}

fn extract_events_from_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();

    if let Some(array) = json.get("events").and_then(|e| e.as_array()) {
        for e in array {
            if let Some(typ) = e.get("type").and_then(|t| t.as_str()) {
                events.push(Event {
                    type_: typ.to_string(),
                    value: serde_json::to_string(e).unwrap_or_default(),
                });
            }
        }
    }

    events
}

fn build_transactions_query(selector: &TransactionsSelector, base: &str) -> (String, usize) {
    let mut query = String::from(base);
    let mut param_count: usize = 0;

    if let Some(range) = &selector.range {
        param_count = 2;

        let ref_query = "(SELECT timestamp FROM explorer_transactions WHERE tx_hash = $1)";

        match range.direction {
            RangeDirection::Next => {
                use std::fmt::Write;
                write!(query, " WHERE (t.timestamp < {ref_query})").unwrap();
                write!(query, " OR (t.timestamp = {ref_query} AND t.tx_hash > $1)").unwrap();
                query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC LIMIT $2");
            }
            RangeDirection::Previous => {
                use std::fmt::Write;
                write!(query, " WHERE (t.timestamp > {ref_query})").unwrap();
                write!(query, " OR (t.timestamp = {ref_query} AND t.tx_hash < $1)").unwrap();
                query.push_str(" ORDER BY t.timestamp ASC, t.tx_hash DESC LIMIT $2");
            }
        }
    } else if selector.latest.is_some() {
        param_count = 1;
        query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC LIMIT $1");
    } else {
        query.push_str(" ORDER BY t.timestamp DESC, t.tx_hash ASC LIMIT 10");
    }

    (query, param_count)
}
