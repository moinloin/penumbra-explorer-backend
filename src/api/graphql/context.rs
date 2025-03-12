use sqlx::PgPool;
use std::sync::Arc;

/// Context for GraphQL resolvers to access shared resources
pub struct Context {
    /// Database connection pool
    pub db: PgPool,
}

impl Context {
    /// Create a new context with the given database pool
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}

//TODO Implement the async_graphql Context marker trait
impl async_graphql::Context for Context {}