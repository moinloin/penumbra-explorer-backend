-- First drop views that depend on tables
DROP VIEW IF EXISTS explorer_recent_blocks;
DROP VIEW IF EXISTS explorer_transaction_summary;

-- Then drop the tables
DROP TABLE IF EXISTS explorer_transactions;
DROP TABLE IF EXISTS explorer_block_details;
DROP TABLE IF EXISTS index_watermarks;
DROP TABLE IF EXISTS cometindex_tracking;

-- Add any other tables that should be dropped here, but keep __diesel_schema_migrations