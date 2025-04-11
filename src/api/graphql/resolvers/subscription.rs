use async_graphql::{Context, Subscription};
use futures_util::stream::Stream;
use tracing;

use crate::api::graphql::{
    pubsub::PubSub,
    types::subscription_types::{BlockUpdate, TransactionCountUpdate, TransactionUpdate},
};

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn blocks(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = BlockUpdate>, async_graphql::Error> {
        let pubsub = PubSub::from_context(ctx)
            .ok_or_else(|| async_graphql::Error::new("PubSub not found in context"))?;
            
        let stream = pubsub
            .blocks_subscribe()
            .into_stream()
            .filter_map(|height| async move {
                match height {
                    Ok(height) => Some(BlockUpdate { height }),
                    Err(e) => {
                        tracing::error!("Error receiving block height: {}", e);
                        None
                    }
                }
            });
            
        Ok(stream)
    }
    
    async fn transactions(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionUpdate>, async_graphql::Error> {
        let pubsub = PubSub::from_context(ctx)
            .ok_or_else(|| async_graphql::Error::new("PubSub not found in context"))?;
            
        let stream = pubsub
            .transactions_subscribe()
            .into_stream()
            .filter_map(|id| async move {
                match id {
                    Ok(id) => Some(TransactionUpdate { id }),
                    Err(e) => {
                        tracing::error!("Error receiving transaction id: {}", e);
                        None
                    }
                }
            });
            
        Ok(stream)
    }
    
    async fn transaction_count(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionCountUpdate>, async_graphql::Error> {
        let pubsub = PubSub::from_context(ctx)
            .ok_or_else(|| async_graphql::Error::new("PubSub not found in context"))?;
            
        let stream = pubsub
            .transaction_count_subscribe()
            .into_stream()
            .filter_map(|count| async move {
                match count {
                    Ok(count) => Some(TransactionCountUpdate { count }),
                    Err(e) => {
                        tracing::error!("Error receiving transaction count: {}", e);
                        None
                    }
                }
            });
            
        Ok(stream)
    }
}