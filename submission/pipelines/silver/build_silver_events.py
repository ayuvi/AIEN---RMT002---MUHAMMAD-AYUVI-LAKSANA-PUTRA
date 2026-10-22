import os
from collections import Counter
from datetime import datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import Json, execute_values


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

SOURCES = {
    "events/payment_events.json": (
        "payment_events",
        ("order_id", "payment_id", "amount"),
    ),
    "events/refund_events.json": (
        "refund_events",
        ("order_id", "refund_id", "amount"),
    ),
    "events/return_events.json": (
        "return_events",
        ("order_id", "return_id"),
    ),
    "events/support_events.json": (
        "support_events",
        ("order_id", "customer_id", "ticket_id", "reason"),
    ),
}

TIMESTAMP_FIELDS = ("occurred_at_utc", "ingested_at_utc")


def parse_timestamp(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing timestamp")

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include timezone")

    return parsed


def parse_amount(value):
    if value is None:
        return None

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("Invalid amount")

    if not amount.is_finite() or amount < 0:
        raise ValueError("Amount must be finite and non-negative")

    return amount


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn:
            with conn.cursor() as cur:
                run_id = os.getenv("PIPELINE_RUN_ID") or datetime.now().strftime(
                    "silver-events-%Y%m%dT%H%M%S"
                )

                for source_file, config in SOURCES.items():
                    table_name, payload_fields = config

                    cur.execute(
                        """
                        SELECT source_record_number, payload, pipeline_run_id
                        FROM bronze.raw_records
                        WHERE source_file = %s
                        ORDER BY source_record_number
                        """,
                        (source_file,),
                    )
                    records = cur.fetchall()
                    accepted = {}
                    rejected = Counter()

                    for record_number, payload, bronze_run_id in records:
                        try:
                            if not isinstance(payload, dict):
                                raise ValueError("Payload must be an object")

                            event_id = payload.get("event_id")
                            event_type = payload.get("event_type")

                            if not event_id or not event_type:
                                raise ValueError("Missing event_id/event_type")

                            occurred_at = parse_timestamp(
                                payload.get("occurred_at_utc")
                            )
                            ingested_at = parse_timestamp(
                                payload.get("ingested_at_utc")
                            )

                            event_payload = payload.get("payload") or {}
                            if not isinstance(event_payload, dict):
                                raise ValueError("Event payload must be an object")

                            values = {
                                field: event_payload.get(field)
                                for field in payload_fields
                            }

                            if "amount" in payload_fields:
                                values["amount"] = parse_amount(
                                    values["amount"]
                                )

                            if table_name in ("payment_events", "refund_events"):
                                columns = [
                                    "event_id", "event_type",
                                    "occurred_at_utc", "ingested_at_utc",
                                    "order_id",
                                    "payment_id" if table_name == "payment_events"
                                    else "refund_id",
                                    "amount", "pipeline_run_id",
                                ]
                                row = (
                                    event_id, event_type, occurred_at,
                                    ingested_at, values["order_id"],
                                    values[columns[5]], values["amount"],
                                    bronze_run_id or run_id,
                                )

                            elif table_name == "return_events":
                                columns = [
                                    "event_id", "event_type",
                                    "occurred_at_utc", "ingested_at_utc",
                                    "order_id", "return_id",
                                    "pipeline_run_id",
                                ]
                                row = (
                                    event_id, event_type, occurred_at,
                                    ingested_at, values["order_id"],
                                    values["return_id"],
                                    bronze_run_id or run_id,
                                )

                            else:
                                columns = [
                                    "event_id", "event_type",
                                    "occurred_at_utc", "ingested_at_utc",
                                    "order_id", "customer_id", "ticket_id",
                                    "reason", "pipeline_run_id",
                                ]
                                row = (
                                    event_id, event_type, occurred_at,
                                    ingested_at, values["order_id"],
                                    values["customer_id"], values["ticket_id"],
                                    values["reason"], bronze_run_id or run_id,
                                )

                            # Keep the first occurrence of each event_id.
                            accepted.setdefault(event_id, row)

                        except (ValueError, TypeError, KeyError) as exc:
                            rejected[str(exc)] += 1

                    if accepted:
                        columns_sql = ", ".join(columns)
                        placeholders = ", ".join(["%s"] * len(columns))

                        update_columns = [
                            col for col in columns if col != "event_id"
                        ]
                        update_sql = ", ".join(
                            f"{col} = EXCLUDED.{col}"
                            for col in update_columns
                        )

                        sql = f"""
                            INSERT INTO silver.{table_name} ({columns_sql})
                            VALUES %s
                            ON CONFLICT (event_id) DO UPDATE SET
                            {update_sql}
                        """

                        execute_values(
                            cur,
                            sql,
                            list(accepted.values()),
                            page_size=1000,
                        )

                    print(f"\nSource: {source_file}")
                    print(f"Records read: {len(records)}")
                    print(f"Unique valid events: {len(accepted)}")
                    print(f"Rejected records: {sum(rejected.values())}")

                    for reason, count in rejected.items():
                        print(f"  - {reason}: {count}")

        print("\nSilver event load completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
