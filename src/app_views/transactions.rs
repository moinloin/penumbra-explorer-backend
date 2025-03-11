use anyhow::Result;
use cometindex::{
    async_trait, index::EventBatch, sqlx, AppView, PgTransaction,
};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::coordination::TransactionQueue;
use crate::parsing::{encode_to_hex, parse_attribute_string};

#[derive(Debug)]
pub struct Transactions {
    tx_queue: Arc<Mutex<TransactionQueue>>,
}

impl Transactions {
    pub fn new(tx_queue: Arc<Mutex<TransactionQueue>>) -> Self {
        Self { tx_queue }
    }

    fn create_transaction_json(
        &self,
        tx_hash: [u8; 32],
        tx_bytes: &[u8],
        height: u64,
        timestamp: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
        tx_index: u64,
        tx_events: &[cometindex::ContextualizedEvent<'_>],
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
        _batch: EventBatch,
    ) -> Result<(), anyhow::Error> {
        let batches = {
            let mut queue = self.tx_queue.lock().await;
            if queue.is_empty() {
                return Ok(());
            }
            queue.take_all_batches()
        };

        let tx_count: usize = batches.iter().map(|b| b.transactions.len()).sum();
        if tx_count == 0 {
            return Ok(());
        }

        tracing::info!("Processing {} transaction batches with {} total transactions",
                      batches.len(), tx_count);

        for batch in batches {
            let height = batch.block_height;
            let timestamp = batch.timestamp;

            tracing::info!("Transactions: Processing batch for block {} with {} transactions",
                          height, batch.transactions.len());

            for tx in batch.transactions {
                tracing::debug!("Transactions: Processing transaction {} in block {}",
                               encode_to_hex(tx.tx_hash), height);

                let decoded_tx_json = self.create_transaction_json(
                    tx.tx_hash,
                    &tx.tx_bytes,
                    height,
                    timestamp,
                    tx.tx_index,
                    &tx.events,
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
                    .bind(tx.tx_hash.as_ref())
                    .bind(i64::try_from(height)?)
                    .bind(timestamp)
                    .bind(&tx.tx_bytes)
                    .bind(decoded_tx_json)
                    .execute(dbtx.as_mut())
                    .await;

                match result {
                    Ok(_) => tracing::debug!("Successfully inserted transaction {}", encode_to_hex(tx.tx_hash)),
                    Err(e) => tracing::error!("Error inserting transaction {}: {:?}", encode_to_hex(tx.tx_hash), e),
                }
            }
        }

        Ok(())
    }
}
