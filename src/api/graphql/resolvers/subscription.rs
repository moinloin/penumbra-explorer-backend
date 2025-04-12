use async_graphql::{Context, Subscription};
use futures_util::stream::Stream;

use crate::api::graphql::{
    pubsub::PubSub,
    types::subscription_types::{BlockUpdate, TransactionCountUpdate, TransactionUpdate},
};

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn blocks(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = BlockUpdate>, async_graphql::Error> {
        let pubsub = ctx.data::<PubSub>()
            .map_err(|_| async_graphql::Error::new("PubSub not found in context"))?;

        let receiver = pubsub.blocks_subscribe();

        Ok(async_stream::stream! {
            let mut receiver = receiver;
            while let Ok(height) = receiver.recv().await {
                yield BlockUpdate { height };
            }
        })
    }

    async fn transactions(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionUpdate>, async_graphql::Error> {
        let pubsub = ctx.data::<PubSub>()
            .map_err(|_| async_graphql::Error::new("PubSub not found in context"))?;

        let receiver = pubsub.transactions_subscribe();

        Ok(async_stream::stream! {
            let mut receiver = receiver;
            while let Ok(id) = receiver.recv().await {
                yield TransactionUpdate { id };
            }
        })
    }

    async fn transaction_count(&self, ctx: &Context<'_>) -> Result<impl Stream<Item = TransactionCountUpdate>, async_graphql::Error> {
        let pubsub = ctx.data::<PubSub>()
            .map_err(|_| async_graphql::Error::new("PubSub not found in context"))?;

        let receiver = pubsub.transaction_count_subscribe();

        Ok(async_stream::stream! {
            let mut receiver = receiver;
            while let Ok(count) = receiver.recv().await {
                yield TransactionCountUpdate { count };
            }
        })
    }
}