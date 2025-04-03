use async_graphql::http::{playground_source, GraphQLPlaygroundConfig};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
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
    schema.execute(req.into_inner()).await.into()
}

pub async fn graphql_playground() -> impl IntoResponse {
    Html(playground_source(GraphQLPlaygroundConfig::new("/graphql")))
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

        let response = rt.block_on(async {
            health_check().await.into_response()
        });

        assert_eq!(response.status(), StatusCode::OK);
    }
    
    #[test]
    fn test_graphql_playground() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let response = rt.block_on(async {
            graphql_playground().await.into_response()
        });

        assert_eq!(response.status(), StatusCode::OK);

        let content_type = response.headers().get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        
        assert!(content_type.contains("text/html"));
    }
}
