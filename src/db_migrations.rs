use anyhow::{Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use std::fs;
use std::path::Path;
use tracing::{info, warn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn run_migrations(database_url: &str) -> Result<()> {
    info!("Connecting to database");
    let mut conn =
        PgConnection::establish(database_url).context("Failed to connect to database")?;

    info!("Resetting database...");
    reset_database(&mut conn)?;

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
    diesel::sql_query("DROP VIEW IF EXISTS explorer_recent_blocks CASCADE").execute(conn)?;
    diesel::sql_query("DROP VIEW IF EXISTS explorer_transaction_summary CASCADE").execute(conn)?;

    diesel::sql_query("DROP TABLE IF EXISTS explorer_transactions CASCADE").execute(conn)?;
    diesel::sql_query("DROP TABLE IF EXISTS explorer_block_details CASCADE").execute(conn)?;

    diesel::sql_query("DROP TABLE IF EXISTS __diesel_schema_migrations CASCADE").execute(conn)?;


    info!("Database reset complete");
    Ok(())
}