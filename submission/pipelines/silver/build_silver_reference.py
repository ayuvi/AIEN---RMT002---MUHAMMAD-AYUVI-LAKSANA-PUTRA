import os
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import psycopg2
from psycopg2.extras import execute_values


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}

# source_file: (table, columns, key columns)
SOURCES = {
    "operational/customers.json": (
        "customers",
        ["customer_id", "city_id", "created_at_utc",
         "customer_segment", "source_record_number", "pipeline_run_id"],
        ["customer_id"],
    ),
    "operational/products.json": (
        "products",
        ["product_id", "product_name", "sku", "unit_price",
         "source_record_number", "pipeline_run_id"],
        ["product_id"],
    ),
    "operational/promotions.json": (
        "promotions",
        ["promotion_id", "promotion_code", "promotion_type",
         "discount_rate", "start_date", "end_date", "pipeline_run_id"],
        ["promotion_id"],
    ),
    "operational/order_promotions.json": (
        "order_promotions",
        ["order_id", "promotion_id", "discount_amount",
         "source_record_number", "pipeline_run_id"],
        ["order_id", "promotion_id"],
    ),
    "inventory/inventory_snapshots.csv": (
        "inventory_snapshots",
        ["product_id", "location_id", "snapshot_date",
         "available_quantity", "reserved_quantity", "unit_cost",
         "pipeline_run_id"],
        ["product_id", "location_id", "snapshot_date"],
    ),
    "reference/campaign_spend.csv": (
        "campaign_spend",
        ["spend_date", "campaign_id", "channel", "spend_amount",
         "pipeline_run_id"],
        ["spend_date", "campaign_id", "channel"],
    ),
}


def decimal_value(value, field, allow_none=False):
    if value in (None, "") and allow_none:
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError(f"{field} is not numeric")
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def required_text(record, field):
    value = record.get(field)
    if value is None or not str(value).strip():
        raise ValueError(f"Missing {field}")
    return str(value).strip()


def parse_date(value, field):
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {field}")


def parse_timestamp(value, field):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Missing {field}")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError(f"Invalid {field}")
    if result.tzinfo is None:
        raise ValueError(f"{field} must include timezone")
    return result


def transform(source, payload, record_number, run_id):
    if not isinstance(payload, dict):
        raise ValueError("Payload must be an object")

    if source == "operational/customers.json":
        return (
            required_text(payload, "customer_id"),
            payload.get("city_id"),
            parse_timestamp(payload["created_at_utc"], "created_at_utc"),
            payload.get("customer_segment"),
            record_number, run_id,
        )

    if source == "operational/products.json":
        price = decimal_value(payload.get("unit_price"), "unit_price")
        if price < 0:
            raise ValueError("unit_price must be non-negative")
        return (
            required_text(payload, "product_id"),
            payload.get("product_name"),
            payload.get("sku"),
            price, record_number, run_id,
        )

    if source == "operational/promotions.json":
        rate = decimal_value(
            payload.get("discount_rate"), "discount_rate", allow_none=True
        )
        if rate is not None and not (0 <= rate <= 1):
            raise ValueError("discount_rate must be between 0 and 1")
        start = parse_date(payload["start_date"], "start_date")
        end = parse_date(payload["end_date"], "end_date")
        if end < start:
            raise ValueError("end_date precedes start_date")
        return (
            required_text(payload, "promotion_id"),
            payload.get("promotion_code"),
            payload.get("promotion_type"),
            rate, start, end, run_id,
        )

    if source == "operational/order_promotions.json":
        amount = decimal_value(
            payload.get("discount_amount"), "discount_amount"
        )
        if amount < 0:
            raise ValueError("discount_amount must be non-negative")
        return (
            required_text(payload, "order_id"),
            required_text(payload, "promotion_id"),
            amount, record_number, run_id,
        )

    if source == "inventory/inventory_snapshots.csv":
        available = int(payload["available_quantity"])
        reserved = int(payload["reserved_quantity"])
        cost = decimal_value(payload.get("unit_cost"), "unit_cost",
                             allow_none=True)
        if available < 0 or reserved < 0:
            raise ValueError("Inventory quantities must be non-negative")
        if cost is not None and cost < 0:
            raise ValueError("unit_cost must be non-negative")
        return (
            required_text(payload, "product_id"),
            required_text(payload, "location_id"),
            parse_date(payload["snapshot_date"], "snapshot_date"),
            available, reserved, cost, run_id,
        )

    if source == "reference/campaign_spend.csv":
        spend = decimal_value(payload.get("spend_amount"), "spend_amount")
        if spend < 0:
            raise ValueError("spend_amount must be non-negative")
        return (
            parse_date(payload["spend_date"], "spend_date"),
            required_text(payload, "campaign_id"),
            required_text(payload, "channel"),
            spend, run_id,
        )

    raise ValueError("Unknown source")


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    run_id = os.getenv("PIPELINE_RUN_ID") or datetime.now().astimezone().strftime(
            "silver-reference-%Y%m%dT%H%M%S%z"
        )

    try:
        with conn:
            with conn.cursor() as cur:
                for source, (table, columns, keys) in SOURCES.items():
                    cur.execute(
                        """
                        SELECT source_record_number, payload
                        FROM bronze.raw_records
                        WHERE source_file = %s
                        ORDER BY source_record_number
                        """,
                        (source,),
                    )
                    records = cur.fetchall()
                    accepted = {}
                    rejected = Counter()

                    for record_number, payload in records:
                        try:
                            row = transform(
                                source, payload, record_number, run_id
                            )
                            key = tuple(row[columns.index(k)] for k in keys)
                            # Deterministically retain the last source row
                            # for duplicate business keys.
                            accepted[key] = row
                        except (ValueError, TypeError, KeyError, OverflowError) as exc:
                            rejected[str(exc)] += 1

                    if accepted:
                        col_sql = ", ".join(columns)
                        key_sql = ", ".join(keys)
                        updates = [
                            col for col in columns if col not in keys
                        ]
                        update_sql = ", ".join(
                            f"{col} = EXCLUDED.{col}" for col in updates
                        )
                        sql = f"""
                            INSERT INTO silver.{table} ({col_sql})
                            VALUES %s
                            ON CONFLICT ({key_sql}) DO UPDATE SET
                            {update_sql}
                        """
                        execute_values(
                            cur, sql, list(accepted.values()), page_size=1000
                        )

                    print(f"\nSource: {source}")
                    print(f"Records read: {len(records)}")
                    print(f"Unique valid records: {len(accepted)}")
                    print(f"Rejected records: {sum(rejected.values())}")
                    for reason, count in rejected.items():
                        print(f"  - {reason}: {count}")

        print("\nSilver reference load completed.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
