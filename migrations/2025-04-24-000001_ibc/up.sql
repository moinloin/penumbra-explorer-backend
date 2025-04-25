-- Drop views first
DROP VIEW IF EXISTS explorer_recent_blocks;
DROP VIEW IF EXISTS explorer_transaction_summary;
DROP VIEW IF EXISTS ibc_client_summary;

-- Drop IBC-related tables with correct dependency order
DROP TABLE IF EXISTS ibc_stats;
DROP TABLE IF EXISTS ibc_channels;
DROP TABLE IF EXISTS ibc_clients;

-- Drop core explorer tables
DROP TABLE IF EXISTS explorer_transactions;
DROP TABLE IF EXISTS explorer_block_details;
DROP TABLE IF EXISTS index_watermarks;
DROP TABLE IF EXISTS cometindex_tracking;

