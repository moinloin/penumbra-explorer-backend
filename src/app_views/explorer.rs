use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};

use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::event::ProtoEvent;
use serde_json::{json, Value};
use sqlx::postgres::PgPool;
use sqlx::types::chrono::DateTime;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use crate::app_views::helpers::transaction::clone_event;
use crate::parsing::{encode_to_hex, event_to_json};

#[derive(Debug)]
pub struct Explorer {
    source_pool: Option<Arc<PgPool>>,
    chain_id: Option<String>,
}

impl Explorer {
    #[must_use]
    pub fn new() -> Self {
        // Try to read chain ID from genesis.json at initialization
        let chain_id = Self::read_chain_id_from_genesis();

        if let Some(id) = &chain_id {
            tracing::info!("Initialized Explorer with chain_id = {}", id);
        } else {
            tracing::warn!("Failed to read chain ID from genesis.json, will use 'unknown'");
        }

        Self {
            source_pool: None,
            chain_id,
        }
    }

    #[must_use]
    pub fn with_source_pool(mut self, pool: Arc<PgPool>) -> Self {
        self.source_pool = Some(pool);
        self
    }

    /// Attempts to read the chain_id from the genesis.json file
    fn read_chain_id_from_genesis() -> Option<String> {
        // Try to open the genesis file
        let file = match File::open("genesis.json") {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to open genesis.json: {}", e);
                return None;
            }
        };

        // Try to read the file contents
        let mut contents = String::new();
        if let Err(e) = file.take(10_000_000).read_to_string(&mut contents) {
            tracing::error!("Failed to read genesis.json: {}", e);
            return None;
        }

        // Try to parse JSON
        let genesis: Result<serde_json::Value, _> = serde_json::from_str(&contents);
        if let Err(e) = genesis {
            tracing::error!("Failed to parse genesis.json: {}", e);
            return None;
        }

        // Extract the chain_id if present
        let genesis = genesis.unwrap();
        let chain_id = genesis["chain_id"].as_str().map(String::from);

        // Log if we couldn't find the chain_id
        if chain_id.is_none() {
            tracing::error!("Could not find chain_id in genesis.json");
        }

        chain_id
    }

    /// Returns the chain ID, using "unknown" if not available
    fn get_chain_id(&self) -> &str {
        self.chain_id.as_deref().unwrap_or("unknown")
    }

    async fn insert_block(
        &self,
        dbtx: &mut PgTransaction<'_>,
        height: u64,
        root: &[u8],
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        tx_count: usize,
        raw_json: &str,
    ) -> Result<(), anyhow::Error> {
        let height_i64 = match i64::try_from(height) {
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
        let chain_id = self.get_chain_id();

        if exists {
            sqlx::query(
                r"
                UPDATE explorer_block_details
                SET
                    root = $2,
                    timestamp = $3,
                    num_transactions = $4,
                    chain_id = $5,
                    raw_json = $6
                WHERE height = $1
                ",
            )
                .bind(height_i64)
                .bind(root)
                .bind(timestamp)
                .bind(i32::try_from(tx_count).unwrap_or(0))
                .bind(chain_id)
                .bind(raw_json)
                .execute(dbtx.as_mut())
                .await?;

            tracing::debug!("Updated block {}", height);
        } else {
            sqlx::query(
                r"
                INSERT INTO explorer_block_details
                (height, root, timestamp, num_transactions, chain_id,
                validator_identity_key, previous_block_hash, block_hash, raw_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ",
            )
                .bind(height_i64)
                .bind(root)
                .bind(timestamp)
                .bind(i32::try_from(tx_count).unwrap_or(0))
                .bind(chain_id)
                .bind(validator_key)
                .bind(previous_hash)
                .bind(block_hash)
                .bind(raw_json)
                .execute(dbtx.as_mut())
                .await?;

            tracing::debug!("Inserted block {}", height);
        }

        Ok(())
    }

    async fn insert_transaction(
        &self,
        dbtx: &mut PgTransaction<'_>,
        tx_hash: &[u8],
        height: u64,
        timestamp: DateTime<sqlx::types::chrono::Utc>,
        fee_amount: u64,
        tx_bytes_base64: &str,
        decoded_tx_json: &str,
    ) -> Result<(), anyhow::Error> {
        let Ok(height_i64) = i64::try_from(height) else {
            return Err(anyhow::anyhow!("Height conversion error: {}", height));
        };

        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM explorer_transactions WHERE tx_hash = $1)",
        )
            .bind(tx_hash)
            .fetch_one(dbtx.as_mut())
            .await?;

        let chain_id = self.get_chain_id();
        let fee_amount_i64 = i64::try_from(fee_amount).unwrap_or(0);

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
                .bind(tx_hash)
                .bind(height_i64)
                .bind(timestamp)
                .bind(fee_amount_i64)
                .bind(chain_id)
                .bind(tx_bytes_base64)
                .bind(decoded_tx_json)
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
                .bind(tx_hash)
                .bind(height_i64)
                .bind(timestamp)
                .bind(fee_amount_i64)
                .bind(chain_id)
                .bind(tx_bytes_base64)
                .bind(decoded_tx_json)
                .execute(dbtx.as_mut())
                .await?;
        }

        Ok(())
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
        tracing::info!("Initializing Explorer with chain_id = {}", self.get_chain_id());

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
                raw_json TEXT
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
                raw_json TEXT,
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
        // Direct access to batch to ensure full processing speed
        let block_results = process_block_events(&batch).await?;
        let blocks_count = block_results.len();

        // Process blocks first (required for foreign key constraints)
        for (height, root, ts, tx_count, _, raw_json, _) in &block_results {
            let raw_json_str = serde_json::to_string_pretty(raw_json)
                .unwrap_or_else(|_| "{}".to_string());

            self.insert_block(
                dbtx,
                *height,
                root,
                *ts,
                *tx_count,
                &raw_json_str,
            ).await?;
        }

        // Process all transactions after blocks
        let mut tx_count = 0;
        for (height, _, timestamp, _, _, _, block_txs) in block_results {
            for (tx_hash, tx_bytes, tx_index, tx_events) in block_txs {
                // Process transaction directly to avoid middleman functions
                let tx_json = create_transaction_json(
                    tx_hash,
                    &tx_bytes,
                    height,
                    timestamp,
                    tx_index,
                    &tx_events
                );

                let tx_bytes_base64 = crate::parsing::encode_to_base64(&tx_bytes);

                // Extract fee amount
                let parsed_json: Value = serde_json::from_str(&tx_json)
                    .unwrap_or_else(|_| json!({}));

                let tx_result_decoded = &parsed_json["transaction_view"];
                let fee_amount = extract_fee_amount(tx_result_decoded);

                // Insert transaction directly
                if let Err(e) = self.insert_transaction(
                    dbtx,
                    &tx_hash,
                    height,
                    timestamp,
                    fee_amount,
                    &tx_bytes_base64,
                    &tx_json,
                ).await {
                    let tx_hash_hex = encode_to_hex(tx_hash);
                    tracing::error!("Error inserting transaction {}: {:?}", tx_hash_hex, e);
                } else {
                    tx_count += 1;
                }
            }
        }

        tracing::info!("Successfully processed {} blocks and {} transactions", blocks_count, tx_count);
        Ok(())
    }
}

// Helper functions for transaction processing
fn extract_fee_amount(tx_result: &Value) -> u64 {
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

fn create_transaction_json(
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    height: u64,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_index: u64,
    tx_events: &[ContextualizedEvent<'_>],
) -> String {
    use crate::app_views::helpers::transaction::decode_transaction;
    use crate::parsing::parse_attribute_string;

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

// This comes directly from your original fast implementation
#[allow(clippy::needless_lifetimes, clippy::unused_async)]
async fn process_block_events<'a>(
    batch: &'a EventBatch,
) -> Result<
    Vec<(
        u64,
        Vec<u8>,
        DateTime<sqlx::types::chrono::Utc>,
        usize,
        Option<String>,
        Value,
        Vec<([u8; 32], Vec<u8>, u64, Vec<ContextualizedEvent<'static>>)>,
    )>,
    anyhow::Error,
> {
    let mut results = Vec::new();

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

        // Chain ID is handled separately now so we don't need to extract it here

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