use async_graphql::{Context, Result, SimpleObject, ComplexObject};
use sqlx::Row;
use crate::api::graphql::{
    context::ApiContext,
    scalars::DateTime,
    types::{Transaction, Event},
};

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct Block {
    pub height: i32,
    pub created_at: DateTime,
    #[graphql(skip)]
    pub raw_json: Option<serde_json::Value>,
}

#[ComplexObject]
impl Block {
    /// Get the number of transactions in this block
    async fn transactions_count(&self, ctx: &Context<'_>) -> Result<i32> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let result = sqlx::query_as::<_, (i32,)>("SELECT num_transactions FROM explorer_block_details WHERE height = $1")
            .bind(self.height as i64)
            .fetch_one(db)
            .await?;

        Ok(result.0)
    }

    /// Get transactions in this block
    async fn transactions(&self, ctx: &Context<'_>) -> Result<Vec<Transaction>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let rows = sqlx::query(
            r#"
            SELECT
                tx_hash,
                block_height,
                timestamp,
                fee_amount,
                chain_id,
                raw_data,
                raw_json
            FROM
                explorer_transactions
            WHERE
                block_height = $1
            ORDER BY
                timestamp ASC
            "#
        )
            .bind(self.height as i64)
            .fetch_all(db)
            .await?;

        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let _block_height: i64 = row.get("block_height");
            let _timestamp: chrono::DateTime<chrono::Utc> = row.get("timestamp");
            let _fee_amount: i64 = row.get("fee_amount");
            let raw_data: Vec<u8> = row.get("raw_data");
            let raw_json: Option<serde_json::Value> = row.get("raw_json");

            if let Some(json) = raw_json {
                let hash = hex::encode_upper(&tx_hash);

                transactions.push(Transaction {
                    hash: hash.clone(),
                    anchor: String::new(),
                    binding_sig: String::new(),
                    index: extract_index_from_json(&json).unwrap_or(0),
                    raw: hex::encode_upper(&raw_data),
                    block: self.clone(),
                    body: crate::api::graphql::types::transaction::extract_transaction_body(&json),
                    raw_events: extract_events_from_json(&json),
                    result: Default::default(),
                });
            }
        }

        Ok(transactions)
    }

    /// Get raw events for this block
    async fn raw_events(&self) -> Result<Vec<Event>> {
        // Extract events from the raw_json
        let events = if let Some(json) = &self.raw_json {
            extract_events_from_block_json(json)
        } else {
            Vec::new()
        };

        Ok(events)
    }
}

impl Block {
    pub fn new(height: i32, created_at: chrono::DateTime<chrono::Utc>, raw_json: Option<serde_json::Value>) -> Self {
        Self {
            height,
            created_at: DateTime(created_at),
            raw_json,
        }
    }

    pub fn from_row(row: sqlx::postgres::PgRow) -> Result<Self> {
        Ok(Self {
            height: row.try_get::<i64, _>("height")? as i32,
            created_at: DateTime(row.try_get("timestamp")?),
            raw_json: row.try_get("raw_json")?,
        })
    }
}

impl Clone for Block {
    fn clone(&self) -> Self {
        Self {
            height: self.height,
            created_at: self.created_at.clone(),
            raw_json: None,
        }
    }
}

// Helper function to extract events from block JSON
fn extract_events_from_block_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();

    if let Some(block) = json.get("block") {
        if let Some(events_array) = block.get("events").and_then(|e| e.as_array()) {
            for event_json in events_array {
                if let Some(event_type) = event_json.get("type").and_then(|t| t.as_str()) {
                    let event_value = serde_json::to_string(event_json).unwrap_or_default();

                    events.push(Event {
                        type_: event_type.to_string(),
                        value: event_value,
                    });
                }
            }
        }
    }

    events
}

// Helper function to extract events from transaction JSON
fn extract_events_from_json(json: &serde_json::Value) -> Vec<Event> {
    let mut events = Vec::new();

    if let Some(events_array) = json.get("events").and_then(|e| e.as_array()) {
        for event_json in events_array {
            if let Some(event_type) = event_json.get("type").and_then(|t| t.as_str()) {
                let event_value = serde_json::to_string(event_json).unwrap_or_default();

                events.push(Event {
                    type_: event_type.to_string(),
                    value: event_value,
                });
            }
        }
    }

    events
}

// Helper function to extract transaction index from JSON
fn extract_index_from_json(json: &serde_json::Value) -> Option<i32> {
    json.get("index")
        .and_then(|i| i.as_str())
        .and_then(|i| i.parse::<i32>().ok())
}