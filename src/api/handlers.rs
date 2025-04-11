use async_graphql::http::{GraphiQLSource, ALL_WEBSOCKET_PROTOCOLS};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    extract::Extension,
    http::StatusCode,
    response::{Html, IntoResponse},
};

use crate::api::graphql::schema::PenumbraSchema;

pub async fn graphql_handler(
    Extension(schema): Extension<PenumbraSchema>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let request = req.into_inner();
    schema.execute(request).await.into()
}

pub async fn graphiql() -> impl IntoResponse {
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/graphql/ws")
            .finish(),
    )
}

pub async fn graphql_subscription(
    Extension(schema): Extension<PenumbraSchema>,
    protocol: GraphQLSubscription<axum::body::BoxBody>,
) -> impl IntoResponse {
    protocol
        .on_connection_init(|value| async {
            tracing::debug!("GraphQL subscription connection initialized: {:?}", value);
            Ok(value)
        })
        .start_with_schema(schema, ALL_WEBSOCKET_PROTOCOLS)
        .await
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_check() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let response = rt.block_on(async { health_check().await.into_response() });

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[test]
    fn test_graphiql() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let response = rt.block_on(async { graphiql().await.into_response() });

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        assert!(content_type.contains("text/html"));
    }
}
