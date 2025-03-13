use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use sqlx::PgPool;

use crate::api::graphql::{
    context::ApiContext,
    resolvers::QueryRoot,
    scalars::register_scalars,
};

/// Type alias for the complete GraphQL schema
pub type PenumbraSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// Create a new GraphQL schema with the given database pool
pub fn create_schema(db_pool: PgPool) -> PenumbraSchema {
    let mut builder = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(ApiContext::new(db_pool));

    // Register custom scalars
    builder = register_scalars(builder);

    builder.finish()
}