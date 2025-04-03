use anyhow::{Context, Result};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::sql_types::BigInt;
use diesel_migrations::{embed_migrations, EmbeddedMigrations, MigrationHarness};
use tracing::{info, warn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Run database migrations to set up the schema
///
/// # Errors
/// Returns an error if database connection fails, if tables can't be checked,
/// or if migrations can't be executed successfully
pub fn run_migrations(database_url: &str) -> Result<()> {
    info!("Connecting to database");
    let mut conn =
        PgConnection::establish(database_url).context("Failed to connect to database")?;

    info!("Running database migrations");

    #[derive(QueryableByName)]
    struct Exists {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        exists: bool,
    }
    
    let table_exists = diesel::sql_query("
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = '__diesel_schema_migrations'
        ) as exists
    ")
    .get_result::<Exists>(&mut conn)?.exists;

    if !table_exists {
        info!("Migration tracking table doesn't exist, creating it");

        diesel::sql_query("
            CREATE TABLE __diesel_schema_migrations (
                version VARCHAR(50) PRIMARY KEY,
                run_on TIMESTAMP NOT NULL DEFAULT NOW()
            )")
            .execute(&mut conn)?;
        
        diesel::sql_query("INSERT INTO __diesel_schema_migrations (version) VALUES ('20250324000001')")
            .execute(&mut conn)?;

        info!("Set up migration tracking table, marked initial schema as applied");
    } else {
        info!("Migration tracking table exists, using existing migration state");
    }

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
