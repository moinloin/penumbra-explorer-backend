use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent;
use serde_json::{json, Value};
use sqlx::types::chrono::DateTime;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::coordination::{PendingTransaction, TransactionQueue};
use crate::parsing::{encode_to_hex, event_to_json};

#[derive(Debug)]
pub struct BlockDetails {
    tx_queue: Arc<Mutex<TransactionQueue>>,
}

struct BlockMetadata<'a> {
    height: u64,
    root: Vec<u8>,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_count: usize,
    chain_id: &'a str,
    raw_json: Value,
}

impl BlockDetails {
    pub fn new(tx_queue: Arc<Mutex<TransactionQueue>>) -> Self {
        Self { tx_queue }
    }

    async fn insert_block(
        &self,
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

        let raw_json_str = serde_json::to_string(&meta.raw_json)?;

        if exists {
            sqlx::query(
                r"
            UPDATE explorer_block_details
            SET
                root = $2,
                timestamp = $3,
                num_transactions = $4,
                chain_id = $5,
                raw_json = $6::jsonb
            WHERE height = $1
            ",
            )
            .bind(height_i64)
            .bind(&meta.root)
            .bind(meta.timestamp)
            .bind(i32::try_from(meta.tx_count).unwrap_or(0))
            .bind(meta.chain_id)
            .bind(&raw_json_str)
            .execute(dbtx.as_mut())
            .await?;

            tracing::debug!("Updated block {}", meta.height);
        } else {
            sqlx::query(
                r"
            INSERT INTO explorer_block_details
            (height, root, timestamp, num_transactions, chain_id,
             validator_identity_key, previous_block_hash, block_hash, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
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
            .bind(&raw_json_str)
            .execute(dbtx.as_mut())
            .await?;

            tracing::debug!("Inserted block {}", meta.height);
        }

        Ok(())
    }
}

#[async_trait]
impl AppView for BlockDetails {
    fn name(&self) -> String {
        "explorer/block_details".to_string()
    }

    async fn init_chain(
        &self,
        _dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    #[allow(clippy::too_many_lines)]
    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
        _ctx: EventBatchContext,
    ) -> Result<(), anyhow::Error> {
        let mut block_data_to_process = Vec::new();

        for block_data in batch.events_by_block() {
            let height = block_data.height();
            let tx_count = block_data.transactions().count();

            tracing::info!(
                "Processing block height {} with {} transactions",
                height,
                tx_count
            );

            let mut block_root = None;
            let mut timestamp = None;
            let mut chain_id: Option<String> = None;
            let mut block_events = Vec::new();
            let mut tx_events = Vec::new();

            let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent>> = HashMap::new();

            for event in block_data.events() {
                if let Ok(pe) = pb::EventBlockRoot::from_event(event.event) {
                    let timestamp_proto = pe.timestamp.unwrap_or_default();
                    timestamp = DateTime::from_timestamp(
                        timestamp_proto.seconds,
                        u32::try_from(timestamp_proto.nanos)?,
                    );
                    block_root = pe.root.map(|r| r.inner);
                }

                let event_json = event_to_json(event, event.tx_hash())?;

                if let Some(tx_hash) = event.tx_hash() {
                    let owned_event = clone_event(event);

                    events_by_tx_hash
                        .entry(tx_hash)
                        .or_default()
                        .push(owned_event);
                    tx_events.push(event_json);
                } else {
                    block_events.push(event_json);
                }
            }

            if tx_count > 0 {
                if let Some((_, tx_bytes)) = block_data.transactions().next() {
                    chain_id = extract_chain_id(tx_bytes);
                }
            }

            let transactions: Vec<Value> = block_data
                .transactions()
                .enumerate()
                .map(|(index, (tx_hash, _))| {
                    json!({
                        "block_id": height,
                        "index": index,
                        "created_at": timestamp,
                        "tx_hash": encode_to_hex(tx_hash)
                    })
                })
                .collect();

            let mut all_events = Vec::new();
            all_events.extend(block_events);
            all_events.extend(tx_events);

            let raw_json = json!({
                "block": {
                    "height": height,
                    "chain_id": chain_id.as_deref().unwrap_or("unknown"),
                    "created_at": timestamp,
                    "transactions": transactions,
                    "events": all_events
                }
            });

            if let (Some(root), Some(ts)) = (block_root, timestamp) {
                block_data_to_process.push((
                    height,
                    root,
                    ts,
                    tx_count,
                    chain_id,
                    raw_json,
                    block_data
                        .transactions()
                        .enumerate()
                        .map(|(tx_index, (tx_hash, tx_bytes))| {
                            (
                                tx_hash,
                                tx_bytes.to_vec(),
                                tx_index as u64,
                                events_by_tx_hash.get(&tx_hash).cloned().unwrap_or_default(),
                            )
                        })
                        .collect::<Vec<_>>(),
                ));
            }
        }

        for (height, root, ts, tx_count, chain_id, raw_json, tx_data) in block_data_to_process {
            let meta = BlockMetadata {
                height,
                root,
                timestamp: ts,
                tx_count,
                chain_id: chain_id.as_deref().unwrap_or("unknown"),
                raw_json,
            };

            self.insert_block(dbtx, meta).await?;

            if !tx_data.is_empty() {
                let mut pending_transactions = Vec::with_capacity(tx_data.len());

                for (tx_hash, tx_bytes, tx_index, tx_events) in tx_data {
                    pending_transactions.push(PendingTransaction {
                        tx_hash,
                        tx_bytes,
                        tx_index,
                        events: tx_events,
                        retry_info: None,
                    });
                }

                let mut queue = self.tx_queue.lock().await;
                queue.create_batch(height, ts, pending_transactions);
                drop(queue);

                tracing::info!(
                    "Queued batch of {} transactions from block {} for processing",
                    tx_count,
                    height
                );
            }
        }

        Ok(())
    }
}

fn clone_event(event: ContextualizedEvent<'_>) -> ContextualizedEvent<'static> {
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

fn extract_chain_id(tx_bytes: &[u8]) -> Option<String> {
    use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
    use prost::Message;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_chain_id_returns_none_for_empty_bytes() {
        let empty_bytes: &[u8] = &[];
        assert_eq!(extract_chain_id(empty_bytes), None);
    }

    #[test]
    fn test_extract_chain_id_returns_none_for_invalid_bytes() {
        let invalid_bytes: &[u8] = &[1, 2, 3, 4, 5];
        assert_eq!(extract_chain_id(invalid_bytes), None);
    }
}
