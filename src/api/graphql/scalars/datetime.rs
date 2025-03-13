use async_graphql::{InputValueResult, InputValueError, Scalar, ScalarType, Value, SchemaBuilder, EmptyMutation, EmptySubscription};
use sqlx::types::chrono::{DateTime as ChronoDateTime, Utc, TimeZone};
use crate::api::graphql::resolvers::QueryRoot;

/// DateTime scalar representing RFC3339 formatted date-times
#[derive(Clone)]
pub struct DateTime(pub ChronoDateTime<Utc>);

#[Scalar]
impl ScalarType for DateTime {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = value {
            match s.parse::<ChronoDateTime<Utc>>() {
                Ok(dt) => Ok(DateTime(dt)),
                Err(_) => Err(InputValueError::custom("Invalid DateTime format")),
            }
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_rfc3339())
    }
}

impl From<ChronoDateTime<Utc>> for DateTime {
    fn from(dt: ChronoDateTime<Utc>) -> Self {
        DateTime(dt)
    }
}

impl From<DateTime> for ChronoDateTime<Utc> {
    fn from(dt: DateTime) -> Self {
        dt.0
    }
}

/// Register the DateTime scalar with the schema
pub fn register(
    builder: SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription>
) -> SchemaBuilder<QueryRoot, EmptyMutation, EmptySubscription> {
    builder.register_scalar_type::<DateTime>("DateTime", |scalar| {
        scalar.description("RFC3339 formatted date-time scalar")
    })
}