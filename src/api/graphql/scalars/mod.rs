mod datetime;
mod u128;

use async_graphql::{SchemaBuilder, EmptyMutation, EmptySubscription};
use crate::api::graphql::resolvers::QueryRoot;

/// Register all custom scalars with the schema
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription>
) -> SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription> {
    let builder = datetime::register(builder);
    let builder = u128::register(builder);
    builder
}

// Re-export the scalar types
pub use datetime::DateTime;
pub use u128::U128;