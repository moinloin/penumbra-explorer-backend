use async_graphql::{EmptyMutation, Schema};
use sqlx::PgPool;

use crate::api::graphql::{
    context::ApiContext,
    pubsub::PubSub,
    resolvers::{QueryRoot, SubscriptionRoot},
    types::{
        Action, Block, BlockUpdate, Event, Fee, Transaction, TransactionBody, TransactionCountUpdate,
        TransactionParameters, TransactionUpdate,
    },
};

/// Type alias for the complete GraphQL schema
#[allow(clippy::module_name_repetitions)]
pub type PenumbraSchema = Schema<QueryRoot, EmptyMutation, SubscriptionRoot>;

/// Create a new GraphQL schema with the given database pool
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    let pubsub = PubSub::new();
    let pool_clone = db_pool.clone();
    let pubsub_clone = pubsub.clone();
    
    tokio::spawn(async move {
        pubsub_clone.start_triggers(pool_clone).await;
    });
    
    let builder = Schema::build(QueryRoot, EmptyMutation, SubscriptionRoot)
        .data(ApiContext::new(db_pool))
        .data(pubsub);

    let builder = builder
        .register_output_type::<Block>()
        .register_output_type::<Transaction>()
        .register_output_type::<TransactionBody>()
        .register_output_type::<TransactionParameters>()
        .register_output_type::<Fee>()
        .register_output_type::<Event>()
        .register_output_type::<Action>()
        .register_output_type::<BlockUpdate>()
        .register_output_type::<TransactionUpdate>()
        .register_output_type::<TransactionCountUpdate>();

    builder.finish()
}
