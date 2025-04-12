use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse, GraphQLSubscription};
use axum::{
    extract::State,
    response::{Html, IntoResponse},
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

pub async fn graphql_subscription(
    State(schema): State<PenumbraSchema>,
    ws: axum::extract::WebSocketUpgrade,
) -> impl IntoResponse {
    ws.protocols(["graphql-ws"])
        .on_upgrade(move |socket| async move {
            // Create a GraphQLSubscription
            let subscription = GraphQLSubscription::new(schema);
            
            // Handle the WebSocket connection
            let (sink, stream) = socket.split();
            
            futures_util::pin_mut!(sink);
            futures_util::pin_mut!(stream);
            
            // Process the WebSocket connection
            subscription.process_stream(stream, sink).await;
        })
}

pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, "OK")
}