use async_graphql::{Context, Subscription};
use futures_util::stream::{Stream, StreamExt};
use std::pin::Pin;

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
}