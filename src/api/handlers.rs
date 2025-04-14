// src/api/handlers.rs
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    response::{Html, IntoResponse},
    extract::State,
    http::StatusCode,
};
use crate::api::graphql::schema::PenumbraSchema;

pub async fn graphql_handler(
    State(schema): State<PenumbraSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.0).await.into()
}

pub async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/graphql/ws")
            .finish(),
    )
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

// We'll use this function in lib.rs to create the subscription service
pub fn create_subscription_service(schema: PenumbraSchema) -> GraphQLSubscription<PenumbraSchema> {
    GraphQLSubscription::new(schema)
}