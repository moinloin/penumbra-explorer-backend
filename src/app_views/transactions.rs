use anyhow::{Context as AnyhowContext, Result};
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, PgTransaction,
};
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use prost::Message;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

use crate::coordination::TransactionQueue;
use crate::parsing::{encode_to_hex, parse_attribute_string};

#[derive(Debug)]
pub struct Transactions {
    tx_queue: Arc<Mutex<TransactionQueue>>,
}

struct TransactionMetadata<'a> {
    tx_hash: [u8; 32],
    height: u64,
    timestamp: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
    fee_amount: u64,
    chain_id: &'a str,
    tx_bytes: &'a [u8],
    decoded_tx_json: Value,
}

impl Transactions {
    pub fn new(tx_queue: Arc<Mutex<TransactionQueue>>) -> Self {
        Self { tx_queue }
    }

    async fn block_exists(dbtx: &mut PgTransaction<'_>, height: u64) -> Result<bool, sqlx::Error> {
        let height_i64 = match i64::try_from(height) {
            Ok(h) => h,
            Err(_) => {
                return Err(sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Height value too large: {}", height),
                ))))
            }
        };

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM explorer_block_details WHERE height = $1)",
        )
        .bind(height_i64)
        .fetch_one(dbtx.as_mut())
        .await?;

        Ok(exists)
    }

    fn extract_fee_amount(&self, tx_result: &Value) -> u64 {
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

    fn extract_chain_id(&self, tx_result: &Value) -> Option<String> {
        tx_result
            .get("body")
            .and_then(|body| body.get("transactionParameters"))
            .and_then(|params| params.get("chainId"))
            .and_then(|chain_id| chain_id.as_str())
            .map(|s| s.to_string())
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

        let tx_result_decoded = self.decode_transaction(tx_hash, tx_bytes);

        let mut ordered_map = Map::with_capacity(7);
        ordered_map.insert("hash".to_string(), json!(encode_to_hex(tx_hash)));
        ordered_map.insert("height".to_string(), json!(height.to_string()));
        ordered_map.insert("index".to_string(), json!(tx_index.to_string()));
        ordered_map.insert("timestamp".to_string(), json!(timestamp));
        ordered_map.insert("tx_result".to_string(), json!(encode_to_hex(tx_bytes)));
        ordered_map.insert("tx_result_decoded".to_string(), tx_result_decoded);
        ordered_map.insert("events".to_string(), json!(processed_events));

        Value::Object(ordered_map)
    }

    fn decode_transaction(&self, tx_hash: [u8; 32], tx_bytes: &[u8]) -> Value {
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

    async fn insert_transaction(
        &self,
        dbtx: &mut PgTransaction<'_>,
        meta: TransactionMetadata<'_>,
    ) -> Result<(), sqlx::Error> {
        let height_i64 = match i64::try_from(meta.height) {
            Ok(h) => h,
            Err(_) => {
                return Err(sqlx::Error::Decode(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Height value too large: {}", meta.height),
                ))))
            }
        };

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM explorer_transactions WHERE tx_hash = $1)",
        )
        .bind(meta.tx_hash.as_ref())
        .fetch_one(dbtx.as_mut())
        .await?;

        let json_str = serde_json::to_string(&meta.decoded_tx_json).map_err(|e| {
            sqlx::Error::Decode(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("JSON serialization error: {}", e),
            )))
        })?;

        if exists {
            sqlx::query(
                r#"
            UPDATE explorer_transactions
            SET
                block_height = $2,
                timestamp = $3,
                fee_amount = $4,
                chain_id = $5,
                raw_data = $6,
                raw_json = $7::jsonb
            WHERE tx_hash = $1
            "#,
            )
            .bind(meta.tx_hash.as_ref())
            .bind(height_i64)
            .bind(meta.timestamp)
            .bind(meta.fee_amount as i64)
            .bind(meta.chain_id)
            .bind(meta.tx_bytes)
            .bind(&json_str)
            .execute(dbtx.as_mut())
            .await?;
        } else {
            sqlx::query(
                r#"
            INSERT INTO explorer_transactions
            (tx_hash, block_height, timestamp, fee_amount, chain_id, raw_data, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            "#,
            )
            .bind(meta.tx_hash.as_ref())
            .bind(height_i64)
            .bind(meta.timestamp)
            .bind(meta.fee_amount as i64)
            .bind(meta.chain_id)
            .bind(meta.tx_bytes)
            .bind(&json_str)
            .execute(dbtx.as_mut())
            .await?;
        }

        Ok(())
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

            let total_batches = queue.len();
            let batches_to_take = std::cmp::min(total_batches, 5);

            if batches_to_take > 0 {
                queue.take_ready_batches()
            } else {
                vec![]
            }
        };

        let tx_count: usize = batches.iter().map(|b| b.transactions.len()).sum();
        if tx_count == 0 {
            return Ok(());
        }

        tracing::info!(
            "Processing {} transaction batches with {} total transactions",
            batches.len(),
            tx_count
        );

        for batch in batches {
            let height = batch.block_height;
            let timestamp = batch.timestamp;
            let start_time = Instant::now();

            if !Self::block_exists(dbtx, height)
                .await
                .context("Failed to check block existence")?
            {
                tracing::info!(
                    "Block {} not yet processed, re-queuing {} transactions",
                    height,
                    batch.transactions.len()
                );

                {
                    let mut queue = self.tx_queue.lock().await;
                    queue.mark_block_failed(height);
                    queue.enqueue_batch(batch.clone());
                    drop(queue);
                }
                continue;
            }

            tracing::info!(
                "Transactions: Processing batch for block {} with {} transactions",
                height,
                batch.transactions.len()
            );

            sqlx::query("SAVEPOINT batch_tx")
                .execute(dbtx.as_mut())
                .await?;

            let mut has_error = false;
            let mut failed_tx_hashes = Vec::new();

            for tx in &batch.transactions {
                let tx_hash_hex = encode_to_hex(tx.tx_hash);
                tracing::debug!("Processing transaction {} in block {}", tx_hash_hex, height);

                let decoded_tx_json = self.create_transaction_json(
                    tx.tx_hash,
                    &tx.tx_bytes,
                    height,
                    timestamp,
                    tx.tx_index,
                    &tx.events,
                );

                let fee_amount = self.extract_fee_amount(&decoded_tx_json["tx_result_decoded"]);

                let chain_id = self
                    .extract_chain_id(&decoded_tx_json["tx_result_decoded"])
                    .unwrap_or_else(|| "unknown".to_string());

                let meta = TransactionMetadata {
                    tx_hash: tx.tx_hash,
                    height,
                    timestamp,
                    fee_amount,
                    chain_id: &chain_id,
                    tx_bytes: &tx.tx_bytes,
                    decoded_tx_json,
                };

                match self.insert_transaction(dbtx, meta).await {
                    Ok(_) => tracing::debug!(
                        "Successfully processed transaction {} with fee {}",
                        tx_hash_hex,
                        fee_amount
                    ),
                    Err(e) => {
                        let is_fk_error = match e.as_database_error() {
                            Some(dbe) => {
                                if let Some(pg_err) =
                                    dbe.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
                                {
                                    pg_err.code() == "23503"
                                } else {
                                    false
                                }
                            }
                            None => false,
                        };

                        if is_fk_error {
                            tracing::warn!(
                                "Block {} not found for transaction {}. Will retry later.",
                                height,
                                tx_hash_hex
                            );
                        } else {
                            tracing::error!("Error inserting transaction {}: {:?}", tx_hash_hex, e);
                        }

                        failed_tx_hashes.push((tx.tx_hash, format!("Error: {:?}", e)));
                        has_error = true;
                    }
                }
            }

            if has_error {
                sqlx::query("ROLLBACK TO SAVEPOINT batch_tx")
                    .execute(dbtx.as_mut())
                    .await?;

                {
                    let mut queue = self.tx_queue.lock().await;
                    if failed_tx_hashes.is_empty() {
                        queue.mark_block_failed(height);
                        queue.enqueue_batch(batch.clone());
                    } else {
                        queue.requeue_batch_with_retries(batch.clone(), &failed_tx_hashes);
                    }
                    drop(queue);
                }
            } else {
                sqlx::query("RELEASE SAVEPOINT batch_tx")
                    .execute(dbtx.as_mut())
                    .await?;

                let elapsed = start_time.elapsed();
                let tx_per_sec = batch.transactions.len() as f64 / elapsed.as_secs_f64();
                tracing::info!(
                    "Completed batch for block {} with {} transactions in {:?} ({:.2} tx/sec)",
                    height,
                    batch.transactions.len(),
                    elapsed,
                    tx_per_sec
                );
            }
        }

        Ok(())
    }
}
