use anyhow::{Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::{info, warn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub fn run_migrations(database_url: &str) -> Result<()> {
    info!("Connecting to database");
    let mut conn =
        PgConnection::establish(database_url).context("Failed to connect to database")?;

    let tables_exist = check_tables_exist(&mut conn)?;

    if !tables_exist {
        info!("Tables don't exist, dropping existing migrations to ensure fresh start");
        diesel::sql_query("DROP TABLE IF EXISTS __diesel_schema_migrations CASCADE").execute(&mut conn)?;
    } else {
        info!("Tables already exist, will run incremental migrations");
    }

    info!("Running migrations");
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

#[derive(QueryableByName)]
struct CountResult {
    #[sql_type = "BigInt"]
    count: i64,
}

fn check_tables_exist(conn: &mut PgConnection) -> Result<bool> {
    let results = diesel::sql_query("
        SELECT COUNT(*) as count FROM information_schema.tables
        WHERE table_name IN ('explorer_block_details', 'explorer_transactions')
        AND table_schema = 'public'")
        .load::<CountResult>(conn)?;

    if let Some(result) = results.first() {
        Ok(result.count == 2)
    } else {
        Ok(false)
    }
}