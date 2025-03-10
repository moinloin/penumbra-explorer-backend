use anyhow::Result;
use cometindex::{
    async_trait, index::EventBatch, sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use penumbra_sdk_proto::{
    core::component::sct::v1 as pb,
    event::ProtoEvent,
    util::tendermint_proxy::v1::GetTxResponse,
};
use sqlx::types::chrono::DateTime;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::parsing::{encode_to_hex, parse_attribute_string};

#[derive(Debug)]
pub struct BlockDetails {
}

impl BlockDetails {
    pub fn new() -> Self {
        Self {}
    }

    fn event_to_json(&self, event: ContextualizedEvent<'_>, tx_hash: Option<[u8; 32]>) -> Result<Value> {
        let mut attributes = Vec::new();

        for attr in &event.event.attributes {
            let attr_str = format!("{:?}", attr);

            attributes.push(json!({
                "key": attr_str.clone(),
                "composite_key": format!("{}.{}", event.event.kind, attr_str),
                "value": "Unknown"
            }));
        }

        let json_event = json!({
            "block_id": event.block_height,
            "tx_id": tx_hash.map(encode_to_hex),
            "type": event.event.kind,
            "attributes": attributes
        });

        Ok(json_event)
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
    ) -> Result<(), anyhow::Error> {
        for block in batch.events_by_block() {
            let mut block_root = None;
            let mut timestamp = None;
            let tx_count = block.transactions().count();
            let height = block.height();

            tracing::info!("Processing block height {} with {} transactions", height, tx_count);

            let mut block_events = Vec::new();
            let mut tx_events = Vec::new();

            let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent>> = HashMap::new();

            for event in block.events() {
                if let Ok(pe) = pb::EventBlockRoot::from_event(&event.event) {
                    let timestamp_proto = pe.timestamp.unwrap_or_default();
                    timestamp = DateTime::from_timestamp(
                        timestamp_proto.seconds,
                        u32::try_from(timestamp_proto.nanos)?,
                    );
                    block_root = Some(pe.root.unwrap().inner);
                }

                if let Some(tx_hash) = event.tx_hash() {
                    events_by_tx_hash.entry(tx_hash).or_default().push(event.clone());
                    tx_events.push(self.event_to_json(event, Some(tx_hash))?);
                } else {
                    block_events.push(self.event_to_json(event, None)?);
                }
            }

            let transactions: Vec<Value> = block.transactions()
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
                    "chain_id": "penumbra-1",
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
                (height, root, timestamp, num_transactions, validator_identity_key, previous_block_hash, block_hash, raw_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (height) DO UPDATE SET
                root = EXCLUDED.root,
                timestamp = EXCLUDED.timestamp,
                num_transactions = EXCLUDED.num_transactions,
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
                    .bind(validator_key)
                    .bind(previous_hash)
                    .bind(block_hash)
                    .bind(raw_json)
                    .execute(dbtx.as_mut())
                    .await?;
            }
        }

        Ok(())
    }
}
