CREATE TABLE IF NOT EXISTS index_watermarks (
                                                app_name TEXT PRIMARY KEY,
                                                height BIGINT NOT NULL,
                                                version BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cometindex_tracking (
                                                   app_name TEXT PRIMARY KEY,
                                                   height BIGINT NOT NULL,
                                                   version BIGINT NOT NULL DEFAULT 1
);

DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = 'index_watermarks'
    ) AND NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'index_watermarks' AND column_name = 'version'
    ) THEN
ALTER TABLE index_watermarks ADD COLUMN version BIGINT NOT NULL DEFAULT 1;
END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_name = 'cometindex_tracking'
    ) AND NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'cometindex_tracking' AND column_name = 'version'
    ) THEN
ALTER TABLE cometindex_tracking ADD COLUMN version BIGINT NOT NULL DEFAULT 1;
END IF;
END
$$;