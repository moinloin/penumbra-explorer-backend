use async_graphql::{Context, Result, Subscription};
use futures_util::{Stream, StreamExt};
use tokio_stream::StreamExt as TokioStreamExt;
use crate::api::graphql::{
    pubsub::PubSub,
    types::subscription_types::{BlockUpdate, TransactionCountUpdate, TransactionUpdate},
    scalars::DateTime,
};
use sqlx::PgPool;
use std::sync::Arc;

#[allow(clippy::module_name_repetitions)]
pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn blocks(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = BlockUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let receiver = pubsub.blocks_subscribe();

        Ok(tokio_stream::wrappers::BroadcastStream::new(receiver)
            .filter_map_unpin(move |result| {
                let pool_clone = Arc::clone(&pool);
                async move {
                    match result {
                        Ok(height) => {
                            match get_block_data(pool_clone, height).await {
                                Ok((created_at, transactions_count)) => {
                                    Some(BlockUpdate {
                                        height,
                                        created_at: DateTime(created_at),
                                        transactions_count
                                    })
                                },
                                Err(e) => {
                                    tracing::error!("Failed to get block data: {}", e);
                                    None
                                }
                            }
                        },
                        Err(_) => None
                    }
                }
            }))
    }

    async fn transactions(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let receiver = pubsub.transactions_subscribe();

        Ok(tokio_stream::wrappers::BroadcastStream::new(receiver)
            .filter_map_unpin(move |result| {
                let pool_clone = Arc::clone(&pool);
                async move {
                    match result {
                        Ok(block_height) => {
                            match get_transaction_data(pool_clone, block_height).await {
                                Ok((hash, raw_data)) => {
                                    Some(TransactionUpdate {
                                        id: block_height,
                                        hash,
                                        raw: raw_data
                                    })
                                },
                                Err(e) => {
                                    tracing::error!("Failed to get transaction data: {}", e);
                                    None
                                }
                            }
                        },
                        Err(_) => None
                    }
                }
            }))
    }

    async fn transaction_count(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionCountUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let receiver = pubsub.transaction_count_subscribe();

        Ok(tokio_stream::wrappers::BroadcastStream::new(receiver)
            .filter_map(|result| async move {
                match result {
                    Ok(count) => Some(TransactionCountUpdate { count }),
                    Err(_) => None
                }
            }))
    }

    async fn latest_blocks(&self, ctx: &Context<'_>, limit: Option<i32>) -> Result<impl Stream<Item = BlockUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let limit = limit.unwrap_or(10);

        let initial_blocks = get_latest_blocks(Arc::clone(&pool), limit).await?;

        let initial_stream = futures_util::stream::iter(initial_blocks);

        let receiver = pubsub.blocks_subscribe();
        let real_time_stream = tokio_stream::wrappers::BroadcastStream::new(receiver)
            .filter_map(move |result| {
                let pool_clone = Arc::clone(&pool);
                async move {
                    match result {
                        Ok(height) => {
                            match get_block_data(pool_clone, height).await {
                                Ok((created_at, transactions_count)) => {
                                    Some(BlockUpdate {
                                        height,
                                        created_at: DateTime(created_at),
                                        transactions_count
                                    })
                                },
                                Err(e) => {
                                    tracing::error!("Failed to get block data: {}", e);
                                    None
                                }
                            }
                        },
                        Err(_) => None
                    }
                }
            });

        let combined_stream = initial_stream.chain(real_time_stream);

        Ok(combined_stream)
    }

    async fn latest_transactions(&self, ctx: &Context<'_>, limit: Option<i32>) -> Result<impl Stream<Item = TransactionUpdate>> {
        let pubsub = ctx.data::<PubSub>()?.clone();
        let pool = Arc::new(ctx.data::<sqlx::PgPool>()?.clone());

        let limit = limit.unwrap_or(10);

        let initial_transactions = get_latest_transactions(Arc::clone(&pool), limit).await?;

        let initial_stream = futures_util::stream::iter(initial_transactions);

        let receiver = pubsub.transactions_subscribe();
        let real_time_stream = tokio_stream::wrappers::BroadcastStream::new(receiver)
            .filter_map(move |result| {
                let pool_clone = Arc::clone(&pool);
                async move {
                    match result {
                        Ok(block_height) => {
                            match get_transaction_data(pool_clone, block_height).await {
                                Ok((hash, raw_data)) => {
                                    Some(TransactionUpdate {
                                        id: block_height,
                                        hash,
                                        raw: raw_data
                                    })
                                },
                                Err(e) => {
                                    tracing::error!("Failed to get transaction data: {}", e);
                                    None
                                }
                            }
                        },
                        Err(_) => None
                    }
                }
            });

        let combined_stream = initial_stream.chain(real_time_stream);

        Ok(combined_stream)
    }
}

async fn get_block_data(pool: Arc<PgPool>, height: i64) -> Result<(chrono::DateTime<chrono::Utc>, i32), sqlx::Error> {
    let row = sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, i32)>(
        "SELECT timestamp, num_transactions FROM explorer_block_details WHERE height = $1"
    )
        .bind(height)
        .fetch_one(pool.as_ref())
        .await?;

    Ok(row)
}

async fn get_transaction_data(pool: Arc<PgPool>, block_height: i64) -> Result<(String, String), sqlx::Error> {
    let row = sqlx::query_as::<_, (Vec<u8>, String)>(
        "SELECT tx_hash, raw_data FROM explorer_transactions WHERE block_height = $1 LIMIT 1"
    )
        .bind(block_height)
        .fetch_one(pool.as_ref())
        .await?;

    let hash_hex = hex::encode_upper(&row.0);

    Ok((hash_hex, row.1))
}

async fn get_latest_blocks(pool: Arc<PgPool>, limit: i32) -> Result<Vec<BlockUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, chrono::DateTime<chrono::Utc>, i32)>(
        "SELECT height, timestamp, num_transactions FROM explorer_block_details
         ORDER BY height DESC LIMIT $1"
    )
        .bind(i64::from(limit))
        .fetch_all(pool.as_ref())
        .await?;

    let blocks = rows.into_iter()
        .map(|(height, timestamp, num_transactions)| {
            BlockUpdate {
                height,
                created_at: DateTime(timestamp),
                transactions_count: num_transactions,
            }
        })
        .collect();

    Ok(blocks)
}

async fn get_latest_transactions(pool: Arc<PgPool>, limit: i32) -> Result<Vec<TransactionUpdate>, sqlx::Error> {
    let rows = sqlx::query_as::<_, (i64, Vec<u8>, String)>(
        "SELECT block_height, tx_hash, raw_data FROM explorer_transactions
         ORDER BY block_height DESC LIMIT $1"
    )
        .bind(i64::from(limit))
        .fetch_all(pool.as_ref())
        .await?;

    let transactions = rows.into_iter()
        .map(|(block_height, tx_hash, raw_data)| {
            let hash = hex::encode_upper(&tx_hash);

            TransactionUpdate {
                id: block_height,
                hash,
                raw: raw_data,
            }
        })
        .collect();

    Ok(transactions)
}