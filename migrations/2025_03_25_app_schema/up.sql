CREATE TABLE IF NOT EXISTS explorer_block_details (
                                                      height BIGINT PRIMARY KEY,
                                                      root BYTEA NOT NULL,
                                                      timestamp TIMESTAMPTZ NOT NULL,
                                                      num_transactions INT NOT NULL DEFAULT 0,
                                                      total_fees NUMERIC(39, 0) DEFAULT 0,
    validator_identity_key TEXT,
    previous_block_hash BYTEA,
    block_hash BYTEA,
    chain_id TEXT,
    raw_json JSONB
    );

CREATE INDEX IF NOT EXISTS idx_explorer_block_details_timestamp ON explorer_block_details(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_explorer_block_details_validator ON explorer_block_details(validator_identity_key);

CREATE TABLE IF NOT EXISTS explorer_transactions (
                                                     tx_hash BYTEA PRIMARY KEY,
                                                     block_height BIGINT NOT NULL,
                                                     timestamp TIMESTAMPTZ NOT NULL,
                                                     fee_amount NUMERIC(39, 0) DEFAULT 0,
    chain_id TEXT,
    raw_data BYTEA,
    raw_json JSONB,
    FOREIGN KEY (block_height) REFERENCES explorer_block_details(height)
    DEFERRABLE INITIALLY DEFERRED
    );

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