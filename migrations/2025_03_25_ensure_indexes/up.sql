-- This migration ensures all indexes and views exist
CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp ON explorer_block_details(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator ON explorer_block_details(validator_identity_key);
CREATE INDEX IF NOT EXISTS idx_explorer_transactions_block_height ON explorer_transactions(block_height);
CREATE INDEX IF NOT EXISTS idx_explorer_transactions_timestamp ON explorer_transactions(timestamp DESC);

CREATE OR REPLACE VIEW explorer_recent_blocks AS
SELECT
    height,
    timestamp,
    num_transactions,
    total_fees,
    validator_identity_key,
    chain_id,
    raw_json
FROM
    explorer_block_details
ORDER BY
    height DESC;

CREATE OR REPLACE VIEW explorer_transaction_summary AS
SELECT
    t.tx_hash,
    t.block_height,
    t.timestamp,
    t.fee_amount,
    t.chain_id,
    t.raw_json
FROM
    explorer_transactions t
ORDER BY
    t.timestamp DESC;