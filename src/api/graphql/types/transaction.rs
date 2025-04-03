use crate::api::graphql::{
    context::ApiContext,
    types::{Action, Block, Event, NotYetSupportedAction},
};
use async_graphql::{Context, Enum, Object, Result};
use sqlx::Row;

#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeDirection {
    #[graphql(name = "NEXT")]
    Next,

    #[graphql(name = "PREVIOUS")]
    Previous,
}

impl Default for RangeDirection {
    fn default() -> Self {
        Self::Next
    }
}

pub struct Transaction {
    pub hash: String,
    pub anchor: String,
    pub binding_sig: String,
    pub index: i32,
    pub raw: String,
    pub block: Block,
    pub body: TransactionBody,
    pub raw_events: Vec<Event>,
    pub raw_json: serde_json::Value,
}

#[Object]
impl Transaction {
    async fn hash(&self) -> &str {
        &self.hash
    }

    async fn anchor(&self) -> &str {
        &self.anchor
    }

    #[graphql(name = "bindingSig")]
    async fn binding_sig(&self) -> &str {
        &self.binding_sig
    }

    async fn index(&self) -> i32 {
        self.index
    }

    async fn raw(&self) -> &str {
        &self.raw
    }

    async fn block(&self) -> &Block {
        &self.block
    }

    async fn body(&self) -> &TransactionBody {
        &self.body
    }

    #[graphql(name = "rawEvents")]
    async fn raw_events(&self) -> &[Event] {
        &self.raw_events
    }

    #[graphql(name = "rawJson")]
    async fn raw_json(&self) -> &serde_json::Value {
        &self.raw_json
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct TransactionBody {
    pub actions: Vec<Action>,
    pub actions_count: i32,
    pub detection_data: Vec<String>,
    pub memo: Option<String>,
    pub parameters: TransactionParameters,
    pub raw_actions: Vec<String>,
}

#[Object]
impl TransactionBody {
    async fn actions(&self) -> &[Action] {
        &self.actions
    }

    #[graphql(name = "actionsCount")]
    async fn actions_count(&self) -> i32 {
        self.actions_count
    }

    #[graphql(name = "detectionData")]
    async fn detection_data(&self) -> &[String] {
        &self.detection_data
    }

    async fn memo(&self) -> &Option<String> {
        &self.memo
    }

    async fn parameters(&self) -> &TransactionParameters {
        &self.parameters
    }

    #[graphql(name = "rawActions")]
    async fn raw_actions(&self) -> &[String] {
        &self.raw_actions
    }
}

#[allow(clippy::module_name_repetitions)]
pub struct TransactionParameters {
    pub chain_id: String,
    pub expiry_height: i32,
    pub fee: Fee,
}

#[Object]
impl TransactionParameters {
    #[graphql(name = "chainId")]
    async fn chain_id(&self) -> &str {
        &self.chain_id
    }

    #[graphql(name = "expiryHeight")]
    async fn expiry_height(&self) -> i32 {
        self.expiry_height
    }

    async fn fee(&self) -> &Fee {
        &self.fee
    }
}

pub struct Fee {
    pub amount: String,
    pub asset_id: Option<crate::api::graphql::types::AssetId>,
}

#[Object]
impl Fee {
    async fn amount(&self) -> &str {
        &self.amount
    }

    #[graphql(name = "assetId")]
    async fn asset_id(&self) -> &Option<crate::api::graphql::types::AssetId> {
        &self.asset_id
    }
}

#[derive(async_graphql::SimpleObject)]
#[allow(clippy::module_name_repetitions)]
pub struct DbRawTransaction {
    pub tx_hash_hex: String,
    pub block_height: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub fee_amount: Option<String>,
    pub chain_id: Option<String>,
    pub raw_data_hex: Option<String>,
    pub raw_json: Option<serde_json::Value>,
}

impl DbRawTransaction {
    /// Gets a transaction by its hash
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_by_hash(ctx: &Context<'_>, tx_hash_hex: String) -> Result<Option<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let Ok(tx_hash_bytes) = hex::decode(tx_hash_hex.trim_start_matches("0x")) else {
            return Ok(None);
        };

        let row_result = sqlx::query(
            r"
            SELECT
                tx_hash,
                block_height,
                timestamp,
                COALESCE(fee_amount::TEXT, '0') as fee_amount,
                chain_id,
                raw_data,
                raw_json
            FROM
                explorer_transactions
            WHERE
                tx_hash = $1
            ",
        )
        .bind(&tx_hash_bytes)
        .fetch_optional(db)
        .await?;

        if let Some(row) = row_result {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let raw_data: Option<String> = row.get("raw_data");

            Ok(Some(Self {
                tx_hash_hex: hex::encode_upper(&tx_hash),
                block_height: row.get("block_height"),
                timestamp: row.get("timestamp"),
                fee_amount: row.get("fee_amount"),
                chain_id: row.get("chain_id"),
                raw_data_hex: raw_data,
                raw_json: row.get("raw_json"),
            }))
        } else {
            Ok(None)
        }
    }

    /// Gets all transactions with pagination
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_all(
        ctx: &Context<'_>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);

        let rows = sqlx::query(
            r"
            SELECT
                tx_hash,
                block_height,
                timestamp,
                COALESCE(fee_amount::TEXT, '0') as fee_amount,
                chain_id,
                raw_data,
                raw_json
            FROM
                explorer_transactions
            ORDER BY
                timestamp DESC
            LIMIT $1 OFFSET $2
            ",
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(db)
        .await?;

        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let raw_data: Option<String> = row.get("raw_data");

            transactions.push(Self {
                tx_hash_hex: hex::encode_upper(&tx_hash),
                block_height: row.get("block_height"),
                timestamp: row.get("timestamp"),
                fee_amount: row.get("fee_amount"),
                chain_id: row.get("chain_id"),
                raw_data_hex: raw_data,
                raw_json: row.get("raw_json"),
            });
        }

        Ok(transactions)
    }
}

#[must_use]
pub fn extract_transaction_body(json: &serde_json::Value) -> TransactionBody {
    let memo = json
        .get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("memo"))
        .and_then(|memo| memo.as_str())
        .map(ToString::to_string);

    let chain_id = json
        .get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("chainId"))
        .and_then(|chain_id| chain_id.as_str())
        .unwrap_or("penumbra-1")
        .to_string();

    let fee_amount = json
        .get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("fee"))
        .and_then(|fee| fee.get("amount"))
        .and_then(|amount| amount.get("lo"))
        .and_then(|lo| lo.as_str())
        .unwrap_or("0")
        .to_string();

    let action = Action::NotYetSupportedAction(NotYetSupportedAction {
        debug: "Transaction action not fully implemented yet".to_string(),
    });

    TransactionBody {
        actions: vec![action],
        actions_count: 1,
        detection_data: Vec::new(),
        memo,
        parameters: TransactionParameters {
            chain_id,
            expiry_height: 0,
            fee: Fee {
                amount: fee_amount,
                asset_id: None,
            },
        },
        raw_actions: Vec::new(),
    }
}
