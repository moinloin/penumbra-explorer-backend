// app_views/explorer.rs
use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use serde_json::{json, Value};
use sqlx::postgres::PgPool;
use sqlx::types::chrono::DateTime;
use std::collections::HashMap;
use std::sync::Arc;

use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent; // Added missing import

use crate::app_views::helpers::{
    block::{create_block_json, fetch_chain_ids_for_blocks, insert_block, BlockMetadata},
    transaction::{clone_event, extract_chain_id_from_bytes, process_transaction},
};
use crate::parsing::{encode_to_hex, event_to_json};

#[derive(Debug, Default)]
pub struct Explorer {
    source_pool: Option<Arc<PgPool>>,
}

impl Explorer {
    #[must_use]
    pub fn new() -> Self {
        Self { source_pool: None }
    }

    #[must_use]
    pub fn with_source_pool(mut self, pool: Arc<PgPool>) -> Self {
        self.source_pool = Some(pool);
        self
    }
}

#[async_trait]
impl AppView for Explorer {
    fn name(&self) -> String {
        "explorer".to_string()
    }

    #[allow(clippy::too_many_lines)]
    async fn init_chain(
        &self,
        dbtx: &mut PgTransaction,
        _: &serde_json::Value,
    ) -> Result<(), anyhow::Error> {
        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS explorer_block_details (
                height BIGINT PRIMARY KEY,
                root BYTEA NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                num_transactions INT NOT NULL DEFAULT 0,
                total_fees NUMERIC(39, 0) DEFAULT 0,
                validator_identity_key TEXT,
                previous_block_hash BYTEA,
                block_hash BYTEA,
                chain_id TEXT,
                raw_json TEXT  -- Changed from JSONB to TEXT
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp
            ON explorer_block_details(timestamp DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator
            ON explorer_block_details(validator_identity_key)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE TABLE IF NOT EXISTS explorer_transactions (
                tx_hash BYTEA PRIMARY KEY,
                block_height BIGINT NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                fee_amount NUMERIC(39, 0) DEFAULT 0,
                chain_id TEXT,
                raw_data TEXT,
                raw_json TEXT,  -- Changed from JSONB to TEXT
                FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
                    DEFERRABLE INITIALLY DEFERRED
            )
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height
            ON explorer_transactions(block_height)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp
            ON explorer_transactions(timestamp DESC)
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW explorer_recent_blocks AS
            SELECT
                height,
                timestamp,
                num_transactions,
                total_fees,
                validator_identity_key,
                chain_id,
                raw_json
            FROM
                explorer_block_details
            ORDER BY
                height DESC
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

        sqlx::query(
            r"
            CREATE OR REPLACE VIEW explorer_transaction_summary AS
            SELECT
                t.tx_hash,
                t.block_height,
                t.timestamp,
                t.fee_amount,
                t.chain_id,
                t.raw_json
            FROM
                explorer_transactions t
            ORDER BY
                t.timestamp DESC
            ",
        )
        .execute(dbtx.as_mut())
        .await?;

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
        let mut transactions_to_process = Vec::new();

        let block_results = process_block_events(self, &batch).await?;

        tracing::info!("Processed {} blocks from batch", block_results.len());

        for (height, root, ts, tx_count, chain_id, raw_json, block_txs) in block_results {
            block_data_to_process.push((height, root, ts, tx_count, chain_id.clone(), raw_json));

            for (tx_hash, tx_bytes, tx_index, tx_events) in block_txs {
                transactions_to_process.push((
                    tx_hash,
                    tx_bytes,
                    tx_index,
                    height,
                    ts,
                    tx_events,
                    chain_id.clone(),
                ));
            }
        }

        for (height, root, ts, tx_count, chain_id, raw_json) in block_data_to_process {
            let meta = BlockMetadata {
                height,
                root,
                timestamp: ts,
                tx_count,
                chain_id: chain_id.as_deref().unwrap_or("unknown"),
                raw_json,
            };

            insert_block(dbtx, meta).await?;
        }

        for (tx_hash, tx_bytes, tx_index, height, timestamp, tx_events, chain_id_opt) in
            transactions_to_process
        {
            process_transaction(
                tx_hash,
                &tx_bytes,
                tx_index,
                height,
                timestamp,
                &tx_events,
                chain_id_opt,
                dbtx,
            )
            .await?;
        }

        Ok(())
    }
}

#[allow(clippy::needless_lifetimes, clippy::unused_async)]
async fn process_block_events<'a>(
    explorer: &Explorer,
    batch: &'a EventBatch,
) -> Result<
    Vec<(
        u64,
        Vec<u8>,
        DateTime<sqlx::types::chrono::Utc>,
        usize,
        Option<String>,
        String,
        Vec<([u8; 32], Vec<u8>, u64, Vec<ContextualizedEvent<'static>>)>,
    )>,
    anyhow::Error,
> {
    let mut results = Vec::new();

    let heights: Vec<u64> = batch
        .events_by_block()
        .map(|block| block.height())
        .collect();

    let chain_ids = fetch_chain_ids_for_blocks(&explorer.source_pool, &heights).await?;
    tracing::info!("Fetched chain IDs for {} blocks in batch", chain_ids.len());

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
        let mut block_events = Vec::new();
        let mut tx_events = Vec::new();

        let mut events_by_tx_hash: HashMap<[u8; 32], Vec<ContextualizedEvent>> = HashMap::new();

        let mut chain_id = chain_ids.get(&height).and_then(|id| id.clone());

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

        if chain_id.is_none() && tx_count > 0 {
            if let Some((_, tx_bytes)) = block_data.transactions().next() {
                chain_id = extract_chain_id_from_bytes(tx_bytes);
                if let Some(ref id) = chain_id {
                    tracing::info!(
                        "Using fallback chain_id = {} from transaction bytes at height {}",
                        id,
                        height
                    );
                }
            }
        }

        if chain_id.is_none() {
            chain_id = Some("penumbra-1".to_string());
            tracing::info!("Using default chain_id = penumbra-1 for height {}", height);
        }

        if let (Some(root), Some(ts)) = (block_root, timestamp) {
            let transactions: Vec<Value> = block_data
                .transactions()
                .enumerate()
                .map(|(index, (tx_hash, _))| {
                    let mut tx_json = serde_json::Map::new();

                    tx_json.insert("index".to_string(), json!(index));
                    tx_json.insert("hash".to_string(), json!(encode_to_hex(tx_hash)));
                    tx_json.insert("timestamp".to_string(), json!(ts.to_rfc3339()));

                    serde_json::Value::Object(tx_json)
                })
                .collect();

            let mut all_events = Vec::new();
            all_events.extend(block_events);
            all_events.extend(tx_events);

            let raw_json = create_block_json(
                height,
                chain_id.as_deref().unwrap_or("penumbra-1"),
                ts,
                &transactions,
                &all_events,
            );

            let mut block_txs = Vec::new();

            for (tx_index, (tx_hash, tx_bytes)) in block_data.transactions().enumerate() {
                let tx_bytes_vec = tx_bytes.to_vec();
                let tx_events = events_by_tx_hash.get(&tx_hash).cloned().unwrap_or_default();

                block_txs.push((tx_hash, tx_bytes_vec, tx_index as u64, tx_events));
            }

            results.push((height, root, ts, tx_count, chain_id, raw_json, block_txs));
        }
    }

    Ok(results)
}
