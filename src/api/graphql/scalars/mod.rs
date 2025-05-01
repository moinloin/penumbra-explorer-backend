mod datetime;
mod bigdecimal;

use crate::api::graphql::resolvers::{QueryRoot, SubscriptionRoot};
use async_graphql::{EmptyMutation, SchemaBuilder};

/// Register all custom scalars with the schema
#[must_use]
#[allow(clippy::module_name_repetitions)]
pub fn register_scalars(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot>,
) -> SchemaBuilder<QueryRoot, EmptyMutation, SubscriptionRoot> {
    let builder = datetime::register(builder);
    bigdecimal::register(builder)
}

pub use datetime::DateTime;
pub use bigdecimal::Decimal; // Export our wrapper type