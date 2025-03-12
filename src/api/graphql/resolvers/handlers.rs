use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::{
    extract::Extension,
    response::{Html, IntoResponse},
    http::StatusCode,
};

use crate::api::graphql::PenumbraSchema;

/// Handler for GraphQL queries and mutations
pub async fn graphql_handler(
    schema: Extension<PenumbraSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

/// Handler for the GraphQL playground interface
pub async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

/// Simple health check endpoint
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "Healthy")
}