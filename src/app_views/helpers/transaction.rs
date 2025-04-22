use anyhow::Result;
use cometindex::ContextualizedEvent;
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value};
use sqlx::{types::chrono::DateTime, PgTransaction};
use std::time::Instant;

use crate::parsing::{encode_to_base64, encode_to_hex, parse_attribute_string};

pub struct TransactionMetadata<'a> {
    pub tx_hash: [u8; 32],
    pub height: u64,
    pub timestamp: DateTime<sqlx::types::chrono::Utc>,
    pub fee_amount: u64,
    pub chain_id: &'a str,
    pub tx_bytes_base64: String,
    pub decoded_tx_json: String,
}

pub fn clone_event(event: ContextualizedEvent<'_>) -> ContextualizedEvent<'static> {
    let event_clone = event.event.clone();

    let tx_clone = event.tx.map(|(hash, bytes)| (hash, bytes.to_vec()));

    ContextualizedEvent {
        block_height: event.block_height,
        event: &*Box::leak(Box::new(event_clone)),
        tx: tx_clone.map(|(hash, bytes)| {
            let static_bytes: &'static [u8] = Box::leak(bytes.into_boxed_slice());
            (hash, static_bytes)
        }),
        local_rowid: event.local_rowid,
    }
}

pub fn extract_fee_amount(tx_result: &Value) -> u64 {
    tx_result
        .get("body")
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("fee"))
        .and_then(|fee| fee.get("amount"))
        .and_then(|amount| amount.get("lo"))
        .and_then(|lo| lo.as_str())
        .and_then(|lo_str| lo_str.parse::<u64>().ok())
        .unwrap_or(0)
}

pub fn extract_chain_id(tx_result: &Value) -> Option<String> {
    tx_result
        .get("body")
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("chainId"))
        .and_then(|chain_id| chain_id.as_str())
        .map(std::string::ToString::to_string)
}

pub fn extract_chain_id_from_bytes(tx_bytes: &[u8]) -> Option<String> {
    match TransactionView::decode(tx_bytes) {
        Ok(tx_view) => {
            if let Some(body) = &tx_view.body_view {
                if let Some(params) = &body.transaction_parameters {
                    return Some(params.chain_id.clone());
                }
            }
        }
        Err(_) => {
            if let Ok(tx) = Transaction::decode(tx_bytes) {
                if let Some(body) = &tx.body {
                    if let Some(params) = &body.transaction_parameters {
                        return Some(params.chain_id.clone());
                    }
                }
            }
        }
    }

    None
}

pub fn decode_transaction(tx_hash: [u8; 32], tx_bytes: &[u8]) -> Value {
    let start = Instant::now();
    let hash_hex = encode_to_hex(tx_hash);

    match TransactionView::decode(tx_bytes) {
        Ok(tx_view) => {
            tracing::debug!(
                "Decoded tx {} with TransactionView in {:?}",
                hash_hex,
                start.elapsed()
            );
            serde_json::to_value(&tx_view).unwrap_or(json!({}))
        }
        Err(e) => {
            tracing::debug!(
                "Error decoding tx {} with TransactionView: {:?}, trying Transaction",
                hash_hex,
                e
            );

            match Transaction::decode(tx_bytes) {
                Ok(tx) => {
                    tracing::debug!(
                        "Decoded tx {} with Transaction in {:?}",
                        hash_hex,
                        start.elapsed()
                    );
                    serde_json::to_value(&tx).unwrap_or(json!({}))
                }
                Err(e2) => {
                    tracing::warn!(
                        "Failed to decode tx {} with both methods: {:?}",
                        hash_hex,
                        e2
                    );
                    json!({})
                }
            }
        }
    }
}

pub fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_index: u64,
    tx_events: &[ContextualizedEvent<'_>],
) -> String {
    let mut processed_events = Vec::with_capacity(tx_events.len() + 1);

    processed_events.push(json!({
        "type": "tx",
        "attributes": [
            {"key": "hash", "value": encode_to_hex(tx_hash)},
            {"key": "height", "value": height.to_string()}
        ]
    }));

    for event in tx_events {
        let attr_capacity = event.event.attributes.len();
        let mut attributes = Vec::with_capacity(attr_capacity);

        for attr in &event.event.attributes {
            let attr_str = format!("{attr:?}");

            if let Some((key, value)) = parse_attribute_string(&attr_str) {
                attributes.push(json!({
                    "key": key,
                    "value": value
                }));
            } else {
                attributes.push(json!({
                    "key": attr_str,
                    "value": "Unknown"
                }));
            }
        }

        processed_events.push(json!({
            "type": event.event.kind,
            "attributes": attributes
        }));
    }

    let tx_result_decoded = decode_transaction(tx_hash, tx_bytes);
    let tx_hash_hex = encode_to_hex(tx_hash);

    let mut tx_json = serde_json::Map::new();
    tx_json.insert("hash".to_string(), json!(tx_hash_hex));
    tx_json.insert("block_height".to_string(), json!(height.to_string()));
    tx_json.insert("index".to_string(), json!(tx_index.to_string()));
    tx_json.insert("timestamp".to_string(), json!(timestamp.to_rfc3339()));
    tx_json.insert("transaction_view".to_string(), tx_result_decoded);
    tx_json.insert("events".to_string(), json!(processed_events));

    let json_value = serde_json::Value::Object(tx_json);
    serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| "{}".to_string())
}
pub async fn insert_transaction(
    dbtx: &mut PgTransaction<'_>,
    meta: TransactionMetadata<'_>,
) -> Result<(), sqlx::Error> {
    let Ok(height_i64) = i64::try_from(meta.height) else {
        return Err(sqlx::Error::Decode(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Height value too large: {}", meta.height),
        ))));
    };

    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM explorer_transactions WHERE tx_hash = $1)",
    )
    .bind(meta.tx_hash.as_ref())
    .fetch_one(dbtx.as_mut())
    .await?;

    if exists {
        sqlx::query(
            r"
        UPDATE explorer_transactions
        SET
            block_height = $2,
            timestamp = $3,
            fee_amount = $4,
            chain_id = $5,
            raw_data = $6,
            raw_json = $7
        WHERE tx_hash = $1
        ",
        )
        .bind(meta.tx_hash.as_ref())
        .bind(height_i64)
        .bind(meta.timestamp)
        .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(&meta.tx_bytes_base64)
        .bind(&meta.decoded_tx_json)
        .execute(dbtx.as_mut())
        .await?;
    } else {
        sqlx::query(
            r"
        INSERT INTO explorer_transactions
        (tx_hash, block_height, timestamp, fee_amount, chain_id, raw_data, raw_json)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ",
        )
        .bind(meta.tx_hash.as_ref())
        .bind(height_i64)
        .bind(meta.timestamp)
        .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
        .bind(meta.chain_id)
        .bind(&meta.tx_bytes_base64)
        .bind(&meta.decoded_tx_json)
        .execute(dbtx.as_mut())
        .await?;
    }

    Ok(())
}

pub async fn process_transaction(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    tx_index: u64,
    height: u64,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_events: &[ContextualizedEvent<'_>],
    chain_id_opt: Option<String>,
    dbtx: &mut PgTransaction<'_>,
) -> Result<(), anyhow::Error> {
    let decoded_tx_json =
        create_transaction_json(tx_hash, tx_bytes, height, timestamp, tx_index, tx_events);

    let parsed_json: Value = serde_json::from_str(&decoded_tx_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse transaction JSON: {}", e))?;

    let tx_result_decoded = &parsed_json["transaction_view"];
    let fee_amount = extract_fee_amount(tx_result_decoded);
    let chain_id = extract_chain_id(tx_result_decoded)
        .or(chain_id_opt)
        .unwrap_or_else(|| "unknown".to_string());

    let tx_bytes_base64 = encode_to_base64(tx_bytes);

    let meta = TransactionMetadata {
        tx_hash,
        height,
        timestamp,
        fee_amount,
        chain_id: &chain_id,
        tx_bytes_base64,
        decoded_tx_json,
    };

    if let Err(e) = insert_transaction(dbtx, meta).await {
        let tx_hash_hex = encode_to_hex(tx_hash);

        let is_fk_error = match e.as_database_error() {
            Some(dbe) => {
                if let Some(pg_err) = dbe.try_downcast_ref::<sqlx::postgres::PgDatabaseError>() {
                    pg_err.code() == "23503"
                } else {
                    false
                }
            }
            None => false,
        };

        if is_fk_error {
            tracing::warn!(
                "Block {} not found for transaction {}. Foreign key constraint failed.",
                height,
                tx_hash_hex
            );
        } else {
            tracing::error!("Error inserting transaction {}: {:?}", tx_hash_hex, e);
        }
    }

    Ok(())
}
