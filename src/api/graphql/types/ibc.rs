use crate::api::graphql::scalars::DateTime;
use async_graphql::{Context, Result, SimpleObject};
use sqlx::Row;

#[derive(SimpleObject)]
#[graphql(name = "IbcStats")]
pub struct Stats {
    pub client_id: String,
    pub shielded_volume: i64,
    pub shielded_tx_count: i64,
    pub unshielded_volume: i64,
    pub unshielded_tx_count: i64,
    pub pending_tx_count: i64,
    pub expired_tx_count: i64,
    #[graphql(name = "lastUpdated")]
    pub last_updated: Option<DateTime>,
}

impl Stats {
    /// Gets IBC stats with optional filtering
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_all(
        ctx: &Context<'_>,
        client_id: Option<String>,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let limit = limit.unwrap_or(100);
        let offset = offset.unwrap_or(0);

        let mut query = String::from(
            "SELECT
                client_id,
                shielded_volume,
                shielded_tx_count,
                unshielded_volume,
                unshielded_tx_count,
                pending_tx_count,
                expired_tx_count,
                last_updated
            FROM ibc_stats",
        );

        if client_id.is_some() {
            query.push_str(" WHERE client_id = $1");
        }

        query.push_str(" ORDER BY client_id");
        query.push_str(&format!(" LIMIT {limit} OFFSET {offset}"));

        let rows = if let Some(client_id_val) = client_id {
            sqlx::query(&query)
                .bind(client_id_val)
                .fetch_all(db)
                .await?
        } else {
            sqlx::query(&query).fetch_all(db).await?
        };

        Ok(rows
            .into_iter()
            .map(|row| Stats {
                client_id: row.get("client_id"),
                shielded_volume: row.get("shielded_volume"),
                shielded_tx_count: row.get("shielded_tx_count"),
                unshielded_volume: row.get("unshielded_volume"),
                unshielded_tx_count: row.get("unshielded_tx_count"),
                pending_tx_count: row.get("pending_tx_count"),
                expired_tx_count: row.get("expired_tx_count"),
                last_updated: row
                    .get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_updated")
                    .map(DateTime),
            })
            .collect())
    }

    /// Gets a specific IBC stats entry by `client_id`
    ///
    /// # Errors
    /// Returns an error if the database query fails
    pub async fn get_by_client_id(ctx: &Context<'_>, client_id: String) -> Result<Option<Self>> {
        let db = &ctx
            .data_unchecked::<crate::api::graphql::context::ApiContext>()
            .db;

        let row = sqlx::query(
            "SELECT
                client_id,
                shielded_volume,
                shielded_tx_count,
                unshielded_volume,
                unshielded_tx_count,
                pending_tx_count,
                expired_tx_count,
                last_updated
            FROM ibc_stats
            WHERE client_id = $1",
        )
        .bind(client_id)
        .fetch_optional(db)
        .await?;

        Ok(row.map(|row| Stats {
            client_id: row.get("client_id"),
            shielded_volume: row.get("shielded_volume"),
            shielded_tx_count: row.get("shielded_tx_count"),
            unshielded_volume: row.get("unshielded_volume"),
            unshielded_tx_count: row.get("unshielded_tx_count"),
            pending_tx_count: row.get("pending_tx_count"),
            expired_tx_count: row.get("expired_tx_count"),
            last_updated: row
                .get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_updated")
                .map(DateTime),
        }))
    }
}
