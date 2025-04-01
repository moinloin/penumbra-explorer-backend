use sqlx::PgPool;

/// Context for GraphQL resolvers to access shared resources
#[allow(clippy::module_name_repetitions)]
pub struct ApiContext {
    /// Database connection pool
    pub db: PgPool,
}

impl ApiContext {
    /// Create a new context with the given database pool
    #[must_use]
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}
