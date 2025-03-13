use sqlx::PgPool;

/// Context for GraphQL resolvers to access shared resources
pub struct ApiContext {
    /// Database connection pool
    pub db: PgPool,
}

impl ApiContext {
    /// Create a new context with the given database pool
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}

impl async_graphql::Context for ApiContext {}
