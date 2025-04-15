use crate::api::graphql::{
    context::ApiContext,
    pubsub::PubSub,
    resolvers::{QueryRoot, SubscriptionRoot},
    scalars,
    types::{
        Action, Block, BlockCollection, BlockUpdate, CollectionItem, Event, Fee, Transaction, TransactionBody,
        TransactionCollection, TransactionCountUpdate, TransactionParameters, TransactionUpdate,
    },
};
use async_graphql::Schema as AsyncGraphQLSchema;
use sqlx::PgPool;

#[allow(clippy::module_name_repetitions)]
pub type PenumbraSchema =
    AsyncGraphQLSchema<QueryRoot, async_graphql::EmptyMutation, SubscriptionRoot>;

#[allow(clippy::module_name_repetitions)]
#[must_use]
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    let pubsub = PubSub::new();
    let pool_clone = db_pool.clone();
    let pubsub_clone = pubsub.clone();

    tokio::spawn(async move {
        pubsub_clone.start_triggers(&pool_clone);
    });

    let builder =
        AsyncGraphQLSchema::build(QueryRoot, async_graphql::EmptyMutation, SubscriptionRoot)
            .data(ApiContext::new(db_pool.clone()))
            .data(pubsub)
            .data(db_pool);

    let builder = scalars::register_scalars(builder);

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
        .register_output_type::<TransactionCountUpdate>()
        .register_output_type::<CollectionItem>()
        .register_output_type::<BlockCollection>()
        .register_output_type::<TransactionCollection>();

    builder.finish()
}
