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

impl BlockDetails {
    pub fn new(tx_queue: Arc<Mutex<TransactionQueue>>) -> Self {
        Self { tx_queue }
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

    async fn index_batch(
        &self,
        dbtx: &mut PgTransaction,
        batch: EventBatch,
        _ctx: EventBatchContext,
    ) -> Result<(), anyhow::Error> {
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
            let mut chain_id = "penumbra-1".to_string(); // Default chain ID
            let mut block_events = Vec::new();
            let mut tx_events = Vec::new();

            let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent<'static>>> =
                HashMap::new();

            for event in block_data.events() {
                if let Ok(pe) = pb::EventBlockRoot::from_event(&event.event) {
                    let timestamp_proto = pe.timestamp.unwrap_or_default();
                    timestamp = DateTime::from_timestamp(
                        timestamp_proto.seconds,
                        u32::try_from(timestamp_proto.nanos)?,
                    );
                    block_root = Some(pe.root.unwrap().inner);
                }

                let event_json = event_to_json(event.clone(), event.tx_hash())?;

                if let Some(tx_hash) = event.tx_hash() {
                    let owned_event = convert_to_static_event(event.clone());
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
                    if let Some(extracted_chain_id) = extract_chain_id(tx_bytes) {
                        chain_id = extracted_chain_id;
                    }
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
                    "chain_id": chain_id,
                    "created_at": timestamp,
                    "transactions": transactions,
                    "events": all_events
                }
            });

            if let (Some(root), Some(ts)) = (block_root, timestamp) {
                let validator_key = None::<String>;
                let previous_hash = None::<Vec<u8>>;
                let block_hash = None::<Vec<u8>>;

                sqlx::query(
                    "
                    INSERT INTO explorer_block_details
                    (height, root, timestamp, num_transactions, chain_id, validator_identity_key, previous_block_hash, block_hash, raw_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (height) DO UPDATE SET
                    root = EXCLUDED.root,
                    timestamp = EXCLUDED.timestamp,
                    num_transactions = EXCLUDED.num_transactions,
                    chain_id = EXCLUDED.chain_id,
                    validator_identity_key = EXCLUDED.validator_identity_key,
                    previous_block_hash = EXCLUDED.previous_block_hash,
                    block_hash = EXCLUDED.block_hash,
                    raw_json = EXCLUDED.raw_json
                    "
                )
                    .bind(i64::try_from(height)?)
                    .bind(root)
                    .bind(ts)
                    .bind(i32::try_from(tx_count)?)
                    .bind(chain_id)
                    .bind(validator_key)
                    .bind(previous_hash)
                    .bind(block_hash)
                    .bind(raw_json)
                    .execute(dbtx.as_mut())
                    .await?;

                if tx_count > 0 {
                    let mut pending_transactions = Vec::with_capacity(tx_count);

                    for (tx_index, (tx_hash, tx_bytes)) in block_data.transactions().enumerate() {
                        let tx_events_for_hash =
                            events_by_tx_hash.get(&tx_hash).cloned().unwrap_or_default();

                        pending_transactions.push(PendingTransaction {
                            tx_hash,
                            tx_bytes: tx_bytes.to_vec(),
                            tx_index: tx_index as u64,
                            events: tx_events_for_hash,
                        });
                    }

                    let mut queue = self.tx_queue.lock().await;
                    queue.create_batch(height, ts, pending_transactions);

                    tracing::info!(
                        "Queued batch of {} transactions from block {} for processing",
                        tx_count,
                        height
                    );
                }
            }
        }

        Ok(())
    }
}

fn convert_to_static_event(event: ContextualizedEvent<'_>) -> ContextualizedEvent<'static> {
    let cloned_event = event.event.clone();
    let boxed_event = Box::new(cloned_event);
    let static_event = Box::leak(boxed_event);

    let static_tx = if let Some((tx_hash, tx_bytes)) = &event.tx {
        let bytes_vec = tx_bytes.to_vec();
        let boxed_bytes = Box::new(bytes_vec);
        let static_bytes_vec = Box::leak(boxed_bytes);
        let static_bytes: &'static [u8] = static_bytes_vec.as_slice();

        Some((*tx_hash, static_bytes))
    } else {
        None
    };

    ContextualizedEvent {
        block_height: event.block_height,
        event: static_event,
        tx: static_tx,
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
        Err(_) => match Transaction::decode(tx_bytes) {
            Ok(tx) => {
                if let Some(body) = &tx.body {
                    if let Some(params) = &body.transaction_parameters {
                        return Some(params.chain_id.clone());
                    }
                }
            }
            Err(_) => {}
        },
    }

    None
}
