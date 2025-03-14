use async_graphql::Result;
use sqlx::Row;
use crate::api::graphql::{
    context::ApiContext,
    types::{Transaction, TransactionsSelector, Block, Event},
};

pub async fn resolve_transaction(ctx: &async_graphql::Context<'_>, hash: String) -> Result<Option<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let hash_bytes = match hex::decode(hash.trim_start_matches("0x")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };

    let row = sqlx::query(
        r#"
        SELECT
            t.tx_hash,
            t.block_height,
            t.timestamp,
            t.fee_amount::TEXT as fee_amount_str, -- Cast NUMERIC to TEXT
            t.chain_id,
            t.raw_data,
            t.raw_json,
            b.timestamp as block_timestamp,
            b.raw_json as block_raw_json
        FROM
            explorer_transactions t
        JOIN
            explorer_block_details b ON t.block_height = b.height
        WHERE
            t.tx_hash = $1
        "#
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
        let raw_data: Vec<u8> = r.get("raw_data");
        let raw_json: Option<serde_json::Value> = r.get("raw_json");

        if let Some(json) = raw_json {
            let hash = hex::encode_upper(&tx_hash);

            Ok(Some(Transaction {
                hash: hash.clone(),
                anchor: String::new(),
                binding_sig: String::new(),
                index: extract_index_from_json(&json).unwrap_or(0),
                raw: hex::encode_upper(&raw_data),
                block: Block::new(
                    block_height as i32,
                    timestamp,
                    None,
                ),
                body: crate::api::graphql::types::extract_transaction_body(&json),
                raw_events: extract_events_from_json(&json),
                result: Default::default(),
            }))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

pub async fn resolve_transactions(ctx: &async_graphql::Context<'_>, selector: TransactionsSelector) -> Result<Vec<Transaction>> {
    let db = &ctx.data_unchecked::<ApiContext>().db;

    let base_query = r#"
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
    "#;

    let (query, _params_count) = build_transactions_query(&selector, base_query);

    if let Some(range) = &selector.range {
        let from_hash_bytes = match hex::decode(range.from_tx_hash.trim_start_matches("0x")) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(vec![]),
        };

        let from_tx_hash = from_hash_bytes.clone();
        let limit = range.limit;

        let rows = sqlx::query(&query)
            .bind(from_tx_hash.as_slice())
            .bind(limit as i64)
            .fetch_all(db)
            .await?;

        return process_transaction_rows(rows);
    } else if let Some(latest) = &selector.latest {
        let limit = latest.limit;

        let rows = sqlx::query(&query)
            .bind(limit as i64)
            .fetch_all(db)
            .await?;

        return process_transaction_rows(rows);
    } else {
        let rows = sqlx::query(&query)
            .fetch_all(db)
            .await?;

        return process_transaction_rows(rows);
    }
}

fn process_transaction_rows(rows: Vec<sqlx::postgres::PgRow>) -> Result<Vec<Transaction>> {
    let mut transactions = Vec::with_capacity(rows.len());

    for r in rows {
        let tx_hash: Vec<u8> = r.get("tx_hash");
        let block_height: i64 = r.get("block_height");
        let timestamp: chrono::DateTime<chrono::Utc> = r.get("timestamp");
        let _fee_amount_str: String = r.get("fee_amount_str");
        let _chain_id: Option<String> = r.get("chain_id");
        let raw_data: Vec<u8> = r.get("raw_data");
        let raw_json: Option<serde_json::Value> = r.get("raw_json");

        if let Some(json) = raw_json.clone() {
            let hash = hex::encode_upper(&tx_hash);

            transactions.push(Transaction {
                hash: hash.clone(),
                anchor: String::new(),
                binding_sig: String::new(),
                index: extract_index_from_json(&json).unwrap_or(0),
                raw: hex::encode_upper(&raw_data),
                block: Block::new(
                    block_height as i32,
                    timestamp,
                    None,
                ),
                body: crate::api::graphql::types::extract_transaction_body(&json),
                raw_events: extract_events_from_json(&json),
                result: Default::default(),
            });
        }
    }

    Ok(transactions)
}

fn extract_index_from_json(json: &serde_json::Value) -> Option<i32> {
    json.get("index")
        .and_then(|i| i.as_str())
        .and_then(|i| i.parse::<i32>().ok())
}

fn extract_events_from_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();

    if let Some(events_array) = json.get("events").and_then(|e| e.as_array()) {
        for event_json in events_array {
            if let Some(event_type) = event_json.get("type").and_then(|t| t.as_str()) {
                let event_value = serde_json::to_string(event_json).unwrap_or_default();

                events.push(Event {
                    type_: event_type.to_string(),
                    value: event_value,
                });
            }
        }
    }

    events
}

fn build_transactions_query(selector: &TransactionsSelector, base_query: &str) -> (String, usize) {
    let mut query = String::from(base_query);
    let param_count;

    if let Some(_range) = &selector.range {
        query.push_str(" WHERE t.timestamp <= (SELECT timestamp FROM explorer_transactions WHERE tx_hash = $1) ORDER BY t.timestamp DESC LIMIT $2");
        param_count = 2;
    } else if let Some(_latest) = &selector.latest {
        query.push_str(" ORDER BY t.timestamp DESC LIMIT $1");
        param_count = 1;
    } else {
        query.push_str(" ORDER BY t.timestamp DESC LIMIT 10");
        param_count = 0;
    }

    (query, param_count)
}
