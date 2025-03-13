use axum::{
    extract::Extension,
    response::{Html, IntoResponse},
    http::StatusCode,
};
use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};

use crate::api::graphql::schema::PenumbraSchema;

pub async fn graphql_handler(
    Extension(schema): Extension<PenumbraSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    schema.execute(req.into_inner()).await.into()
}

pub async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}
