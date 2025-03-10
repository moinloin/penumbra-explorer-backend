use anyhow::Result;
use cometindex::{
    async_trait, index::EventBatch, sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use penumbra_sdk_proto::{
    core::{
        component::sct::v1 as pb,
        transaction::v1::{Transaction, TransactionView},
    },
};
use prost::Message;
use sqlx::types::chrono::DateTime;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::parsing::{encode_to_hex, parse_attribute_string};

#[derive(Debug)]
pub struct Transactions {
}

impl Transactions {
    pub fn new() -> Self {
        Self {}
    }

    async fn get_block_timestamp(&self, dbtx: &mut PgTransaction<'_>, height: u64) -> Result<Option<DateTime<sqlx::types::chrono::Utc>>, anyhow::Error> {
        let timestamp: Option<DateTime<sqlx::types::chrono::Utc>> = sqlx::query_scalar(
            "SELECT timestamp FROM explorer_block_details WHERE height = $1"
        )
            .bind(i64::try_from(height)?)
            .fetch_optional(dbtx.as_mut())
            .await?;

        Ok(timestamp)
    }

    fn create_transaction_json(
        &self,
        tx_hash: [u8; 32],
        tx_bytes: &[u8],
        height: u64,
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        tx_index: u64,
        tx_events: &[ContextualizedEvent<'_>],
    ) -> Value {
        let mut processed_events = Vec::new();

        processed_events.push(json!({
            "type": "tx",
            "attributes": [
                {"key": "hash", "value": encode_to_hex(tx_hash)},
                {"key": "height", "value": height.to_string()}
            ]
        }));

        for event in tx_events {
            let mut attributes = Vec::new();

            for attr in &event.event.attributes {
                let attr_str = format!("{:?}", attr);

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

        let tx_result_decoded = match TransactionView::decode(tx_bytes) {
            Ok(tx_view) => {
                serde_json::to_value(&tx_view).unwrap_or(json!({}))
            },
            Err(e) => {
                tracing::debug!("Error decoding transaction with hash {} using TransactionView: {:?}",
                         encode_to_hex(tx_hash), e);

                match Transaction::decode(tx_bytes) {
                    Ok(tx) => {
                        tracing::debug!("Successfully decoded transaction with hash {} using Transaction",
                                 encode_to_hex(tx_hash));
                        serde_json::to_value(&tx).unwrap_or(json!({}))
                    },
                    Err(e2) => {
                        tracing::warn!("Error decoding transaction with hash {} using Transaction: {:?}",
                                 encode_to_hex(tx_hash), e2);
                        json!({})
                    }
                }
            }
        };

        let mut ordered_json = serde_json::Map::new();
        ordered_json.insert("hash".to_string(), json!(encode_to_hex(tx_hash)));
        ordered_json.insert("height".to_string(), json!(height.to_string()));
        ordered_json.insert("index".to_string(), json!(tx_index.to_string()));
        ordered_json.insert("timestamp".to_string(), json!(timestamp));
        ordered_json.insert("tx_result".to_string(), json!(encode_to_hex(tx_bytes)));
        ordered_json.insert("tx_result_decoded".to_string(), json!(tx_result_decoded));
        ordered_json.insert("events".to_string(), json!(processed_events));

        Value::Object(ordered_json)
    }
}

#[async_trait]
impl AppView for Transactions {
    fn name(&self) -> String {
        "explorer/transactions".to_string()
    }

    async fn init_chain(
        &self,
        _dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
    ) -> Result<(), anyhow::Error> {
        for block in batch.events_by_block() {
            let height = block.height();
            let tx_count = block.transactions().count();

            tracing::info!("Transactions: Processing block height {} with {} transactions", height, tx_count);

            if tx_count == 0 {
                continue;
            }

            let timestamp = self.get_block_timestamp(dbtx, height).await?;

            if timestamp.is_none() {
                tracing::warn!("Transactions: No timestamp found for block height {}, skipping transactions", height);
                continue;
            }

            let block_time = timestamp.unwrap();

            let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent>> = HashMap::new();

            for event in block.events() {
                if let Some(tx_hash) = event.tx_hash() {
                    events_by_tx_hash.entry(tx_hash).or_default().push(event.clone());
                }
            }

            for (tx_index, (tx_hash, tx_bytes)) in block.transactions().enumerate() {
                tracing::debug!("Transactions: Processing transaction {} in block {}", encode_to_hex(tx_hash), height);

                let tx_events = events_by_tx_hash.get(&tx_hash).cloned().unwrap_or_default();

                let decoded_tx_json = self.create_transaction_json(
                    tx_hash,
                    tx_bytes,
                    height,
                    block_time,
                    tx_index as u64,
                    &tx_events
                );

                let result = sqlx::query(
                    "
                    INSERT INTO explorer_transactions
                    (tx_hash, block_height, timestamp, raw_data, raw_json)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (tx_hash) DO UPDATE SET
                    block_height = EXCLUDED.block_height,
                    timestamp = EXCLUDED.timestamp,
                    raw_data = EXCLUDED.raw_data,
                    raw_json = EXCLUDED.raw_json
                    "
                )
                    .bind(tx_hash.as_ref())
                    .bind(i64::try_from(height)?)
                    .bind(block_time)
                    .bind(tx_bytes)
                    .bind(decoded_tx_json)
                    .execute(dbtx.as_mut())
                    .await;

                match result {
                    Ok(_) => tracing::debug!("Transactions: Successfully inserted transaction {}", encode_to_hex(tx_hash)),
                    Err(e) => tracing::error!("Transactions: Error inserting transaction {}: {:?}", encode_to_hex(tx_hash), e),
                }
            }
        }

        Ok(())
    }
}
