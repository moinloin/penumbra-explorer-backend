use anyhow::Result;
use cometindex::{
    async_trait, index::{EventBatch, EventBatchContext}, sqlx, AppView, PgTransaction,
};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Value, Map};
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

    fn extract_fee_amount(&self, tx_result: &Value) -> u64 {
        if let Some(body) = tx_result.get("body") {
            if let Some(params) = body.get("transactionParameters") {
                if let Some(fee) = params.get("fee") {
                    if let Some(amount) = fee.get("amount") {
                        if let Some(lo) = amount.get("lo") {
                            if let Some(lo_str) = lo.as_str() {
                                if let Ok(fee_amount) = lo_str.parse::<u64>() {
                                    return fee_amount;
                                }
                            }
                        }
                    }
                }
            }
        }

        0
    }

    fn extract_chain_id(&self, tx_result: &Value) -> Option<String> {
        if let Some(body) = tx_result.get("body") {
            if let Some(params) = body.get("transactionParameters") {
                if let Some(chain_id) = params.get("chainId") {
                    if let Some(chain_id_str) = chain_id.as_str() {
                        return Some(chain_id_str.to_string());
                    }
                }
            }
        }

        None
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

        let mut ordered_map = Map::new();
        ordered_map.insert("hash".to_string(), json!(encode_to_hex(tx_hash)));
        ordered_map.insert("height".to_string(), json!(height.to_string()));
        ordered_map.insert("index".to_string(), json!(tx_index.to_string()));
        ordered_map.insert("timestamp".to_string(), json!(timestamp));
        ordered_map.insert("tx_result".to_string(), json!(encode_to_hex(tx_bytes)));
        ordered_map.insert("tx_result_decoded".to_string(), tx_result_decoded.clone());

        ordered_map.insert("events".to_string(), json!(processed_events));

        Value::Object(ordered_map)
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
        _ctx: EventBatchContext,
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

                let fee_amount = self.extract_fee_amount(&decoded_tx_json["tx_result_decoded"]);
                let chain_id = self.extract_chain_id(&decoded_tx_json["tx_result_decoded"])
                    .unwrap_or_else(|| "penumbra-1".to_string());

                let result = sqlx::query(
                    "
                    INSERT INTO explorer_transactions
                    (tx_hash, block_height, timestamp, fee_amount, chain_id, raw_data, raw_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (tx_hash) DO UPDATE SET
                    block_height = EXCLUDED.block_height,
                    timestamp = EXCLUDED.timestamp,
                    fee_amount = EXCLUDED.fee_amount,
                    chain_id = EXCLUDED.chain_id,
                    raw_data = EXCLUDED.raw_data,
                    raw_json = EXCLUDED.raw_json
                    "
                )
                    .bind(tx.tx_hash.as_ref())
                    .bind(i64::try_from(height)?)
                    .bind(timestamp)
                    .bind(fee_amount as i64)
                    .bind(chain_id)
                    .bind(&tx.tx_bytes)
                    .bind(decoded_tx_json)
                    .execute(dbtx.as_mut())
                    .await;

                match result {
                    Ok(_) => tracing::debug!("Successfully inserted transaction {} with fee {}",
                                            encode_to_hex(tx.tx_hash), fee_amount),
                    Err(e) => tracing::error!("Error inserting transaction {}: {:?}", encode_to_hex(tx.tx_hash), e),
                }
            }
        }

        Ok(())
    }
}
