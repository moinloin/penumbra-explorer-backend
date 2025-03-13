mod datetime;

use async_graphql::{SchemaBuilder, EmptyMutation, EmptySubscription};
use crate::api::graphql::resolvers::QueryRoot;

/// Register all custom scalars with the schema
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription>
) -> SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription> {
    let builder = datetime::register(builder);
    builder
}

pub use datetime::DateTime;
