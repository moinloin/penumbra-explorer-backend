use anyhow::{Context, Result};
use diesel::prelude::*;
use diesel::pg::PgConnection;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::fs;
use std::path::Path;
use tracing::{info, warn};

// Embed all migrations from the migrations directory
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn run_migrations(database_url: &str) -> Result<()> {
    // Connect to the database
    info!("Connecting to database");
    let mut conn = PgConnection::establish(database_url)
        .context("Failed to connect to database")?;

    // Reset the database by explicitly dropping all objects first
    info!("Resetting database...");
    reset_database(&mut conn)?;

    // Run the migrations (this will create everything fresh)
    info!("Running migrations to create fresh schema");
    match conn.run_pending_migrations(MIGRATIONS) {
        Ok(applied) => {
            info!("Successfully applied {} migrations", applied.len());
        }
        Err(e) => {
            warn!("Failed to run migrations: {:?}", e);
            return Err(anyhow::anyhow!("Migration error: {:?}", e));
        }
    }

    Ok(())
}

fn reset_database(conn: &mut PgConnection) -> Result<()> {
    // Drop views first to avoid dependency issues
    diesel::sql_query("DROP VIEW IF EXISTS explorer_recent_blocks CASCADE").execute(conn)?;
    diesel::sql_query("DROP VIEW IF EXISTS explorer_transaction_summary CASCADE").execute(conn)?;

    // Drop tables in correct order to respect foreign keys
    diesel::sql_query("DROP TABLE IF EXISTS explorer_transactions CASCADE").execute(conn)?;
    diesel::sql_query("DROP TABLE IF EXISTS explorer_block_details CASCADE").execute(conn)?;

    // Drop schema_version table if it exists (diesel's internal tracking table)
    diesel::sql_query("DROP TABLE IF EXISTS __diesel_schema_migrations CASCADE").execute(conn)?;

    info!("Database reset complete");
    Ok(())
}