use async_graphql::{SimpleObject, Context, Result};
use sqlx::Row;
use crate::api::graphql::{
    context::ApiContext,
    types::{Block, DbBlock},
};

#[derive(SimpleObject)]
pub struct Transaction {
    pub hash: String,
    pub anchor: String,
    pub binding_sig: String,
    pub index: i32,
    pub raw: String,
    pub block: Block,
    pub body: TransactionBody,
    pub raw_events: Vec<crate::api::graphql::types::Event>,
    pub result: TransactionResult,
}

#[derive(SimpleObject)]
pub struct TransactionBody {
    pub actions: Vec<crate::api::graphql::types::Action>,
    pub actions_count: i32,
    pub detection_data: Vec<String>,
    pub memo: Option<String>,
    pub parameters: TransactionParameters,
    pub raw_actions: Vec<String>,
}

#[derive(SimpleObject)]
pub struct TransactionParameters {
    pub chain_id: String,
    pub expiry_height: i32,
    pub fee: Fee,
}

#[derive(SimpleObject)]
pub struct Fee {
    pub amount: String,
    pub asset_id: Option<crate::api::graphql::types::asset::AssetId>,
}

#[derive(SimpleObject, Default)]
pub struct TransactionResult {
    pub code: i32,
    pub codespace: String,
    pub data: String,
    pub events: Vec<String>,
    pub gas_used: i32,
    pub gas_wanted: i32,
    pub info: String,
    pub log: String,
}

#[derive(SimpleObject)]
pub struct DbTransaction {
    pub tx_hash_hex: String,
    pub block_height: i64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub fee_amount: Option<String>,
    pub chain_id: Option<String>,
    pub raw_data_hex: Option<String>,
    pub json: Option<serde_json::Value>,
}

impl DbTransaction {
    pub async fn get_by_hash(ctx: &Context<'_>, tx_hash_hex: String) -> Result<Option<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let tx_hash_bytes = match hex::decode(tx_hash_hex.trim_start_matches("0x")) {
            Ok(bytes) => bytes,
            Err(_) => return Ok(None),
        };

        let row_result = sqlx::query(
            r#"
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
            "#
        )
            .bind(&tx_hash_bytes)
            .fetch_optional(db)
            .await?;

        if let Some(row) = row_result {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let raw_data: Option<Vec<u8>> = row.get("raw_data");

            Ok(Some(Self {
                tx_hash_hex: hex::encode_upper(&tx_hash),
                block_height: row.get("block_height"),
                timestamp: row.get("timestamp"),
                fee_amount: row.get("fee_amount"),
                chain_id: row.get("chain_id"),
                raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
                json: row.get("raw_json"),
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_all(ctx: &Context<'_>, limit: Option<i64>, offset: Option<i64>) -> Result<Vec<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let limit = limit.unwrap_or(10);
        let offset = offset.unwrap_or(0);

        let rows = sqlx::query(
            r#"
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
            "#
        )
            .bind(limit)
            .bind(offset)
            .fetch_all(db)
            .await?;

        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let raw_data: Option<Vec<u8>> = row.get("raw_data");

            transactions.push(Self {
                tx_hash_hex: hex::encode_upper(&tx_hash),
                block_height: row.get("block_height"),
                timestamp: row.get("timestamp"),
                fee_amount: row.get("fee_amount"),
                chain_id: row.get("chain_id"),
                raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
                json: row.get("raw_json"),
            });
        }

        Ok(transactions)
    }

    pub async fn get_by_block(ctx: &Context<'_>, block_height: i64) -> Result<Vec<Self>> {
        let db = &ctx.data_unchecked::<ApiContext>().db;

        let rows = sqlx::query(
            r#"
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
                block_height = $1
            ORDER BY
                timestamp ASC
            "#
        )
            .bind(block_height)
            .fetch_all(db)
            .await?;

        let mut transactions = Vec::with_capacity(rows.len());

        for row in rows {
            let tx_hash: Vec<u8> = row.get("tx_hash");
            let raw_data: Option<Vec<u8>> = row.get("raw_data");

            transactions.push(Self {
                tx_hash_hex: hex::encode_upper(&tx_hash),
                block_height: row.get("block_height"),
                timestamp: row.get("timestamp"),
                fee_amount: row.get("fee_amount"),
                chain_id: row.get("chain_id"),
                raw_data_hex: raw_data.map(|data| hex::encode_upper(&data)),
                json: row.get("raw_json"),
            });
        }

        Ok(transactions)
    }

    pub async fn get_block(&self, ctx: &Context<'_>) -> Result<Option<DbBlock>> {
        DbBlock::get_by_height(ctx, self.block_height).await
    }
}

pub fn extract_transaction_body(json: &serde_json::Value) -> TransactionBody {
    let memo = json.get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("memo"))
        .and_then(|memo| memo.as_str())
        .map(|s| s.to_string());

    let chain_id = json.get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("chainId"))
        .and_then(|chain_id| chain_id.as_str())
        .unwrap_or("penumbra-1")
        .to_string();

    let fee_amount = json.get("tx_result_decoded")
        .and_then(|tx| tx.get("body"))
        .and_then(|body| body.get("transactionParameters"))
        .and_then(|params| params.get("fee"))
        .and_then(|fee| fee.get("amount"))
        .and_then(|amount| amount.get("lo"))
        .and_then(|lo| lo.as_str())
        .unwrap_or("0")
        .to_string();

    TransactionBody {
        actions: Vec::new(),
        actions_count: 0,
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
