use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use sqlx::PgPool;

use crate::api::graphql::{
    context::ApiContext,
    resolvers::QueryRoot,
    types::{Action, Block, Event, Fee, Transaction, TransactionBody, TransactionParameters},
};

/// Type alias for the complete GraphQL schema
#[allow(clippy::module_name_repetitions)]
pub type PenumbraSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Create a new GraphQL schema with the given database pool
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    let builder =
        Schema::build(QueryRoot, EmptyMutation, EmptySubscription).data(ApiContext::new(db_pool));

    let builder = builder
        .register_output_type::<Block>()
        .register_output_type::<Transaction>()
        .register_output_type::<TransactionBody>()
        .register_output_type::<TransactionParameters>()
        .register_output_type::<Fee>()
        .register_output_type::<Event>()
        .register_output_type::<Action>();

    builder.finish()
}
