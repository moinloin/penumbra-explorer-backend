use std::sync::Arc;
use anyhow::Result;
use cometindex::{
    async_trait,
    index::{EventBatch, EventBatchContext},
    sqlx, AppView, ContextualizedEvent, PgTransaction,
};
use sqlx::postgres::PgPool;

use penumbra_sdk_proto::core::component::sct::v1 as pb;
use penumbra_sdk_proto::core::transaction::v1::{Transaction, TransactionView};
use penumbra_sdk_proto::event::ProtoEvent;
use prost::Message;
use serde_json::{json, Map, Value};
use sqlx::types::chrono::DateTime;
use std::collections::HashMap;
use std::time::Instant;

use crate::parsing::{encode_to_base64, encode_to_hex, event_to_json, parse_attribute_string};

#[derive(Debug)]
pub struct Explorer {
    source_pool: Option<Arc<PgPool>>,
}

struct BlockMetadata<'a> {
    height: u64,
    root: Vec<u8>,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_count: usize,
    chain_id: &'a str,
    raw_json: Value,
}

struct TransactionMetadata<'a> {
    tx_hash: [u8; 32],
    height: u64,
    timestamp: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
    fee_amount: u64,
    chain_id: &'a str,
    tx_bytes_base64: String,
    decoded_tx_json: Value,
}

impl Default for Explorer {
    fn default() -> Self {
        Self {
            source_pool: None,
        }
    }
}

impl Explorer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            source_pool: None,
        }
    }

    pub fn with_source_pool(mut self, pool: Arc<PgPool>) -> Self {
        self.source_pool = Some(pool);
        self
    }

    async fn fetch_chain_id_for_block(&self, height: u64) -> Result<Option<String>, anyhow::Error> {
        if let Some(pool) = &self.source_pool {
            let chain_id = sqlx::query_scalar::<_, Option<String>>(
                "SELECT chain_id FROM blocks WHERE height = $1"
            )
                .bind(i64::try_from(height)?)
                .fetch_optional(pool.as_ref())
                .await?;

            Ok(chain_id.flatten())
        } else {
            Ok(None)
        }
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

    async fn insert_transaction(
        &self,
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

        let json_str = serde_json::to_string(&meta.decoded_tx_json).map_err(|e| {
            sqlx::Error::Decode(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("JSON serialization error: {e}"),
            )))
        })?;

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
                raw_json = $7::jsonb
            WHERE tx_hash = $1
            ",
            )
                .bind(meta.tx_hash.as_ref())
                .bind(height_i64)
                .bind(meta.timestamp)
                .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
                .bind(meta.chain_id)
                .bind(&meta.tx_bytes_base64)
                .bind(&json_str)
                .execute(dbtx.as_mut())
                .await?;
        } else {
            sqlx::query(
                r"
            INSERT INTO explorer_transactions
            (tx_hash, block_height, timestamp, fee_amount, chain_id, raw_data, raw_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            ",
            )
                .bind(meta.tx_hash.as_ref())
                .bind(height_i64)
                .bind(meta.timestamp)
                .bind(i64::try_from(meta.fee_amount).unwrap_or(0))
                .bind(meta.chain_id)
                .bind(&meta.tx_bytes_base64)
                .bind(&json_str)
                .execute(dbtx.as_mut())
                .await?;
        }

        Ok(())
    }

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

    fn extract_chain_id(tx_result: &Value) -> Option<String> {
        tx_result
            .get("body")
            .and_then(|body| body.get("transactionParameters"))
            .and_then(|params| params.get("chainId"))
            .and_then(|chain_id| chain_id.as_str())
            .map(std::string::ToString::to_string)
    }

    fn decode_transaction(tx_hash: [u8; 32], tx_bytes: &[u8]) -> Value {
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

    fn create_transaction_json(
        tx_hash: [u8; 32],
        tx_bytes: &[u8],
        height: u64,
        timestamp: sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>,
        tx_index: u64,
        tx_events: &[ContextualizedEvent<'_>],
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

        let tx_result_decoded = Self::decode_transaction(tx_hash, tx_bytes);

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
}

fn extract_chain_id_from_bytes(tx_bytes: &[u8]) -> Option<String> {
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
                raw_json JSONB
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
                raw_json JSONB,
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

        // Pass self to process_block_events to access source_pool
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

            self.insert_block(dbtx, meta).await?;
        }

        for (tx_hash, tx_bytes, tx_index, height, timestamp, tx_events, chain_id_opt) in
            transactions_to_process
        {
            process_transaction(
                self,
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

chain_id = match explorer.fetch_chain_id_for_block(height).await {
Ok(Some(id)) => {
Some(id)
},
_ => None,
};

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
tracing::info!("Using fallback chain_id = {} from transaction bytes at height {}", id, height);
}
}
}

if chain_id.is_none() {
chain_id = Some("penumbra-1".to_string());
tracing::info!("Using default chain_id = penumbra-1 for height {}", height);
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
"chain_id": chain_id.as_deref().unwrap_or("penumbra-1"),
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

#[allow(clippy::too_many_arguments)]
async fn process_transaction(
    explorer: &Explorer,
    tx_hash: [u8; 32],
    tx_bytes: &[u8],
    tx_index: u64,
    height: u64,
    timestamp: DateTime<sqlx::types::chrono::Utc>,
    tx_events: &[ContextualizedEvent<'_>],
    chain_id_opt: Option<String>,
    dbtx: &mut PgTransaction<'_>,
) -> Result<(), anyhow::Error> {
    let decoded_tx_json = Explorer::create_transaction_json(
        tx_hash, tx_bytes, height, timestamp, tx_index, tx_events,
    );

    let fee_amount = Explorer::extract_fee_amount(&decoded_tx_json["tx_result_decoded"]);
    let chain_id = Explorer::extract_chain_id(&decoded_tx_json["tx_result_decoded"])
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

    if let Err(e) = explorer.insert_transaction(dbtx, meta).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_extract_chain_id_from_bytes_returns_none_for_empty_bytes() {
        let empty_bytes: &[u8] = &[];
        assert_eq!(extract_chain_id_from_bytes(empty_bytes), None);
    }

    #[test]
    fn test_extract_chain_id_from_bytes_returns_none_for_invalid_bytes() {
        let invalid_bytes: &[u8] = &[1, 2, 3, 4, 5];
        assert_eq!(extract_chain_id_from_bytes(invalid_bytes), None);
    }

    #[test]
    fn test_extract_fee_amount() {
        let tx_result = json!({
            "body": {
                "transactionParameters": {
                    "fee": {
                        "amount": {
                            "lo": "1000"
                        }
                    }
                }
            }
        });

        assert_eq!(Explorer::extract_fee_amount(&tx_result), 1000);

        let tx_result_missing_fee = json!({
            "body": {
                "transactionParameters": {}
            }
        });

        assert_eq!(Explorer::extract_fee_amount(&tx_result_missing_fee), 0);

        let empty_json = json!({});
        assert_eq!(Explorer::extract_fee_amount(&empty_json), 0);
    }

    #[test]
    fn test_extract_chain_id() {
        let tx_result = json!({
            "body": {
                "transactionParameters": {
                    "chainId": "penumbra-testnet"
                }
            }
        });

        assert_eq!(
            Explorer::extract_chain_id(&tx_result),
            Some("penumbra-testnet".to_string())
        );

        let tx_result_missing_chain_id = json!({
            "body": {
                "transactionParameters": {}
            }
        });

        assert_eq!(
            Explorer::extract_chain_id(&tx_result_missing_chain_id),
            None
        );

        let empty_json = json!({});
        assert_eq!(Explorer::extract_chain_id(&empty_json), None);
    }

    #[test]
    fn test_decode_transaction_with_invalid_data() {
        let tx_hash = [0u8; 32];
        let invalid_tx_bytes = vec![1, 2, 3, 4];

        let result = Explorer::decode_transaction(tx_hash, &invalid_tx_bytes);

        assert!(result.is_object());
        assert!(result.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_create_transaction_json() {
        let tx_hash = [1u8; 32];
        let tx_bytes = vec![1, 2, 3, 4];
        let height = 100;
        let timestamp = Utc::now();
        let tx_index = 2;
        let tx_events = vec![];

        let result = Explorer::create_transaction_json(
            tx_hash, &tx_bytes, height, timestamp, tx_index, &tx_events,
        );

        let obj = result.as_object().unwrap();

        assert_eq!(
            obj.get("hash").unwrap().as_str().unwrap(),
            "0101010101010101010101010101010101010101010101010101010101010101"
        );
        assert_eq!(obj.get("height").unwrap().as_str().unwrap(), "100");
        assert_eq!(obj.get("index").unwrap().as_str().unwrap(), "2");
        assert!(obj.contains_key("timestamp"));
        assert_eq!(obj.get("tx_result").unwrap().as_str().unwrap(), "01020304");
        assert!(obj.contains_key("tx_result_decoded"));
        assert!(obj.contains_key("events"));

        let events = obj.get("events").unwrap().as_array().unwrap();
        assert_eq!(events.len(), 1);

        let tx_event = &events[0];
        assert_eq!(tx_event.get("type").unwrap(), "tx");

        let attrs = tx_event.get("attributes").unwrap().as_array().unwrap();
        assert_eq!(attrs.len(), 2);

        assert_eq!(attrs[0].get("key").unwrap(), "hash");
        assert_eq!(
            attrs[0].get("value").unwrap(),
            "0101010101010101010101010101010101010101010101010101010101010101"
        );

        assert_eq!(attrs[1].get("key").unwrap(), "height");
        assert_eq!(attrs[1].get("value").unwrap(), "100");
    }
}
