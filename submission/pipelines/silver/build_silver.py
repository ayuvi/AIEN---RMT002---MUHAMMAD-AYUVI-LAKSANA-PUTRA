
import os
import uuid
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

SOURCE_FILE = "operational/orders.json"


def parse_timestamp(value):
    """Convert ISO timestamp text into a timezone-aware datetime."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing or invalid timestamp")

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include timezone information")

    return parsed


def validate_order(payload):
    """Return a rejection reason if the order is invalid."""
    for field in ("order_id", "customer_id", "status",
                  "ordered_at_utc", "sales_channel",
                  "shipping_revenue"):
        value = payload.get(field)

        if value is None or (
            isinstance(value, str) and not value.strip()
        ):
            return f"Missing required field: {field}"

    try:
        ordered_at = parse_timestamp(payload["ordered_at_utc"])

        updated_value = payload.get("updated_at_utc")
        updated_at = (
            parse_timestamp(updated_value)
            if updated_value not in (None, "")
            else None
        )

        shipping = Decimal(str(payload["shipping_revenue"]))

        if not shipping.is_finite():
            return "shipping_revenue must be a finite number"

        if shipping < 0:
            return "shipping_revenue cannot be negative"

        if updated_at and updated_at < ordered_at:
            return "updated_at_utc is earlier than ordered_at_utc"

    except (ValueError, InvalidOperation, TypeError):
        return "Invalid timestamp or shipping_revenue format"

    return None


def main():
    run_id = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    source_record_number,
                    source_record_id,
                    payload
                FROM bronze.raw_records
                WHERE source_file = %s
                ORDER BY source_record_number
                """,
                (SOURCE_FILE,),
            )
            source_rows = cur.fetchall()

            if not source_rows:
                raise RuntimeError(
                    f"No records found for {SOURCE_FILE}"
                )

            valid_orders = {}
            rejected_orders = []

            for row_number, source_id, payload in source_rows:
                reason = validate_order(payload)

                if reason:
                    rejected_orders.append(
                        (
                            SOURCE_FILE,
                            row_number,
                            source_id,
                            payload.get("order_id"),
                            reason,
                            Json(payload),
                            run_id,
                        )
                    )
                    continue

                order_id = payload["order_id"]
                updated_value = payload.get("updated_at_utc")
                updated_at = (
                    parse_timestamp(updated_value)
                    if updated_value
                    else None
                )

                candidate = {
                    "source_record_number": row_number,
                    "source_record_id": source_id,
                    "payload": payload,
                    "updated_at": updated_at,
                }

                current = valid_orders.get(order_id)

                # Keep the newest version. If timestamps tie,
                # keep the earliest source row deterministically.
                if current is None:
                    valid_orders[order_id] = candidate
                elif (
                    (updated_at is not None)
                    and (
                        current["updated_at"] is None
                        or updated_at > current["updated_at"]
                    )
                ):
                    valid_orders[order_id] = candidate

            # Rebuild these outputs from the current source snapshot.
            # Bronze remains untouched.
            cur.execute("DELETE FROM silver.orders")
            cur.execute(
                """
                DELETE FROM silver.rejected_records
                WHERE source_file = %s
                """,
                (SOURCE_FILE,),
            )

            order_values = []

            for order_id, item in valid_orders.items():
                payload = item["payload"]

                order_values.append(
                    (
                        order_id,
                        payload["customer_id"],
                        payload["status"],
                        parse_timestamp(payload["ordered_at_utc"]),
                        item["updated_at"],
                        payload["sales_channel"],
                        payload.get("store_id"),
                        Decimal(str(payload["shipping_revenue"])),
                        item["source_record_number"],
                        run_id,
                    )
                )

            if order_values:
                execute_values(
                    cur,
                    """
                    INSERT INTO silver.orders (
                        order_id,
                        customer_id,
                        order_status,
                        ordered_at_utc,
                        updated_at_utc,
                        sales_channel,
                        store_id,
                        shipping_revenue,
                        source_record_number,
                        pipeline_run_id
                    ) VALUES %s
                    """,
                    order_values,
                )

            if rejected_orders:
                execute_values(
                    cur,
                    """
                    INSERT INTO silver.rejected_records (
                        source_file,
                        source_record_number,
                        source_record_id,
                        order_id,
                        rejection_reason,
                        payload,
                        pipeline_run_id
                    ) VALUES %s
                    """,
                    rejected_orders,
                )

            print("Silver orders pipeline completed.")
            print(f"Pipeline run ID: {run_id}")
            print(f"Bronze records read: {len(source_rows)}")
            print(f"Unique valid orders: {len(valid_orders)}")
            print(f"Rejected records: {len(rejected_orders)}")


if __name__ == "__main__":
    main()
