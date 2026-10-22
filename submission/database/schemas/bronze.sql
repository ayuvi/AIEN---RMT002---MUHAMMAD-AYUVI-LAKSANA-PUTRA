CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    record_id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_format TEXT NOT NULL,
    source_record_number BIGINT NOT NULL,
    source_record_id TEXT,
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_bronze_source_record
        UNIQUE (source_file, source_record_number, payload_hash)
);

CREATE INDEX IF NOT EXISTS idx_bronze_source_file
    ON bronze.raw_records (source_file);

CREATE INDEX IF NOT EXISTS idx_bronze_source_record_id
    ON bronze.raw_records (source_record_id);

CREATE INDEX IF NOT EXISTS idx_bronze_pipeline_run
    ON bronze.raw_records (pipeline_run_id);
