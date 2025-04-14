// src/api/graphql/scalars/mod.rs
mod datetime;
use crate::api::graphql::resolvers::{QueryRoot, SubscriptionRoot};
use async_graphql::{EmptyMutation, SchemaBuilder};

/// Register all custom scalars with the schema
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot> {
    datetime::register(builder)
}

pub use datetime::DateTime;