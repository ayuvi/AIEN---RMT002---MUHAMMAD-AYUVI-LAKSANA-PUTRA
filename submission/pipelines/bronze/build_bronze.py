import csv
import hashlib
import json
import os
import uuid
from pathlib import Path

import psycopg2
from psycopg2.extras import Json, execute_values

PROJECT_DIR = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_DIR / "data" / "raw"

DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

ID_FIELDS = (
    "source_row_id",
    "event_id",
    "order_id",
    "customer_id",
    "product_id",
    "order_item_id",
    "promotion_id",
    "address_id",
    "store_id",
    "channel_id",
    "category_id",
    "city_id",
)


def load_records(path):
    """Read one JSON or CSV file and yield dictionary records."""
    if path.suffix.lower() == ".json":
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = [data]
        else:
            raise ValueError(f"Unsupported JSON structure: {path}")

        for record in records:
            if not isinstance(record, dict):
                raise ValueError(f"Expected JSON object in {path}")
            yield record

    elif path.suffix.lower() == ".csv":
        with path.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            if not reader.fieldnames:
                raise ValueError(f"CSV has no header: {path}")

            for record in reader:
                yield record


def get_source_id(record):
    for field in ID_FIELDS:
        value = record.get(field)
        if value not in (None, ""):
            return str(value)
    return None


def get_payload_hash(record):
    canonical = json.dumps(
        record,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def main():
    if not RAW_DIR.is_dir():
        raise FileNotFoundError(f"Raw data folder not found: {RAW_DIR}")

    files = sorted(
        path
        for path in RAW_DIR.rglob("*")
        if path.is_file() and path.suffix.lower() in {".json", ".csv"}
    )

    if not files:
        raise RuntimeError(f"No JSON or CSV files found in {RAW_DIR}")

    run_id = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())
    total_read = 0
    total_inserted = 0

    print(f"Pipeline run ID: {run_id}")
    print(f"Raw data folder: {RAW_DIR}")
    print(f"Files found: {len(files)}")

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                        UNIQUE (
                            source_file,
                            source_record_number,
                            payload_hash
                        )
                );
                """
            )

            for path in files:
                relative_path = path.relative_to(RAW_DIR).as_posix()
                file_count = 0
                file_inserted = 0
                batch = []

                for row_number, record in enumerate(load_records(path), start=1):
                    total_read += 1
                    file_count += 1

                    batch.append(
                        (
                            relative_path,
                            path.suffix.lower().lstrip("."),
                            row_number,
                            get_source_id(record),
                            Json(record),
                            get_payload_hash(record),
                            run_id,
                        )
                    )

                    if len(batch) >= 1000:
                        inserted_rows = execute_values(
                            cur,
                            """
                            INSERT INTO bronze.raw_records (
                                source_file,
                                source_format,
                                source_record_number,
                                source_record_id,
                                payload,
                                payload_hash,
                                pipeline_run_id
                            ) VALUES %s
                            ON CONFLICT (
                                source_file,
                                source_record_number,
                                payload_hash
                            ) DO NOTHING
                            RETURNING record_id
                            """,
                            batch,
                            fetch=True,
                        )
                        file_inserted += len(inserted_rows)
                        batch.clear()

                if batch:
                    inserted_rows = execute_values(
                        cur,
                        """
                        INSERT INTO bronze.raw_records (
                            source_file,
                            source_format,
                            source_record_number,
                            source_record_id,
                            payload,
                            payload_hash,
                            pipeline_run_id
                        ) VALUES %s
                        ON CONFLICT (
                            source_file,
                            source_record_number,
                            payload_hash
                        ) DO NOTHING
                        RETURNING record_id
                        """,
                        batch,
                        fetch=True,
                    )
                    file_inserted += len(inserted_rows)

                total_inserted += file_inserted
                print(
                    f"{relative_path}: "
                    f"read={file_count}, inserted={file_inserted}"
                )

    print("\nBronze ingestion completed.")
    print(f"Run ID: {run_id}")
    print(f"Total records read: {total_read}")
    print(f"Total records inserted: {total_inserted}")
    print(f"Existing records skipped: {total_read - total_inserted}")


if __name__ == "__main__":
    main()
