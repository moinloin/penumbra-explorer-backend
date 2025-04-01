mod datetime;

use crate::api::graphql::resolvers::QueryRoot;
use async_graphql::{EmptyMutation, EmptySubscription, SchemaBuilder};

/// Register all custom scalars with the schema
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription> {
    datetime::register(builder)
}

pub use datetime::DateTime;
