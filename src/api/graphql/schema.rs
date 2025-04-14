// src/api/graphql/schema.rs
use async_graphql::Schema;
use sqlx::PgPool;
use crate::api::graphql::{
    context::ApiContext,
    pubsub::PubSub,
    resolvers::{QueryRoot, SubscriptionRoot},
    types::{
        Action, Block, BlockUpdate, Event, Fee, Transaction, TransactionBody, TransactionCountUpdate,
        TransactionParameters, TransactionUpdate,
    },
    scalars,
};

/// Type alias for the complete GraphQL schema
pub type PenumbraSchema = Schema<QueryRoot, async_graphql::EmptyMutation, SubscriptionRoot>;

/// Create a new GraphQL schema with the given database pool
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    let pubsub = PubSub::new();
    let pool_clone = db_pool.clone();
    let pubsub_clone = pubsub.clone();

    // Spawn the background task for subscription triggers
    tokio::spawn(async move {
        pubsub_clone.start_triggers(pool_clone).await;
    });

    // Build and register the Schema
    let builder = Schema::build(QueryRoot, async_graphql::EmptyMutation, SubscriptionRoot)
        .data(ApiContext::new(db_pool.clone()))
        .data(pubsub)
        .data(db_pool); // Add the raw database pool to the context for subscription resolvers

    // Register scalar types
    let builder = scalars::register_scalars(builder);

    // Register output types
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