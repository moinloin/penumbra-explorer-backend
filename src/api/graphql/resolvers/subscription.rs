use async_graphql::{Context, Subscription};
use futures_util::stream::{Stream, StreamExt};
use std::pin::Pin;

use crate::api::graphql::{
    pubsub::PubSub,
    types::subscription_types::{BlockUpdate, TransactionCountUpdate, TransactionUpdate},
};

pub struct SubscriptionRoot;