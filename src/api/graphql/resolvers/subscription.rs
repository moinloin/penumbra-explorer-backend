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
            
        let receiver = pubsub.blocks_subscribe();
        
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(height) => yield BlockUpdate { height },
                    Err(e) => {
                        tracing::error!("Error receiving block height: {}", e);
                    }
                }
            }
        };
            
        Ok(stream)
    }
    
    async fn transactions(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionUpdate>, async_graphql::Error> {
        let pubsub = PubSub::from_context(ctx)
            .ok_or_else(|| async_graphql::Error::new("PubSub not found in context"))?;
            
        let receiver = pubsub.transactions_subscribe();
        
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(id) => yield TransactionUpdate { id },
                    Err(e) => {
                        tracing::error!("Error receiving transaction id: {}", e);
                    }
                }
            }
        };
            
        Ok(stream)
    }
    
    async fn transaction_count(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionCountUpdate>, async_graphql::Error> {
        let pubsub = PubSub::from_context(ctx)
            .ok_or_else(|| async_graphql::Error::new("PubSub not found in context"))?;
            
        let receiver = pubsub.transaction_count_subscribe();
        
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok(count) => yield TransactionCountUpdate { count },
                    Err(e) => {
                        tracing::error!("Error receiving transaction count: {}", e);
                    }
                }
            }
        };
            
        Ok(stream)
    }
}