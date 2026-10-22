
import os
import uuid
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

ITEMS_FILE = "operational/order_items.json"
PRODUCTS_FILE = "operational/products.json"


def validate_item(payload, order_ids, product_ids):
    """Return a rejection reason when an item fails validation."""

    required_fields = (
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
    )

    for field in required_fields:
        value = payload.get(field)

        if value is None or (
            isinstance(value, str) and not value.strip()
        ):
            return f"Missing required field: {field}"

    try:
        quantity = int(payload["quantity"])

        if str(payload["quantity"]).strip() != str(quantity):
            return "quantity must be an integer"

        if quantity <= 0:
            return "quantity must be greater than zero"

        unit_price = Decimal(str(payload["unit_price"]))
        discount = Decimal(str(payload.get("item_discount_amount", 0)))

        if not unit_price.is_finite() or unit_price < 0:
            return "unit_price must be a finite non-negative number"

        if not discount.is_finite() or discount < 0:
            return "item_discount_amount must be a finite non-negative number"

    except (ValueError, TypeError, InvalidOperation):
        return "Invalid quantity, unit_price, or discount format"

    if payload["order_id"] not in order_ids:
        return "Order not found in silver.orders"

    if payload["product_id"] not in product_ids:
        return "Product not found in operational/products.json"

    return None


def main():
    run_id = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:

            # Load valid parent orders.
            cur.execute("SELECT order_id FROM silver.orders")
            order_ids = {row[0] for row in cur.fetchall()}

            # Load product IDs from the Bronze product snapshot.
            cur.execute(
                """
                SELECT DISTINCT payload ->> 'product_id'
                FROM bronze.raw_records
                WHERE source_file = %s
                  AND NULLIF(payload ->> 'product_id', '') IS NOT NULL
                """,
                (PRODUCTS_FILE,),
            )
            product_ids = {row[0] for row in cur.fetchall()}

            # Read the original order-item records.
            cur.execute(
                """
                SELECT source_record_number, source_record_id, payload
                FROM bronze.raw_records
                WHERE source_file = %s
                ORDER BY source_record_number
                """,
                (ITEMS_FILE,),
            )
            source_rows = cur.fetchall()

            if not source_rows:
                raise RuntimeError(f"No records found for {ITEMS_FILE}")

            valid_items = {}
            rejected_items = []

            for row_number, source_id, payload in source_rows:
                reason = validate_item(payload, order_ids, product_ids)

                if reason:
                    rejected_items.append(
                        (
                            ITEMS_FILE,
                            row_number,
                            source_id,
                            payload.get("order_item_id"),
                            reason,
                            Json(payload),
                            run_id,
                        )
                    )
                    continue

                item_id = payload["order_item_id"]

                candidate = {
                    "row_number": row_number,
                    "source_id": source_id,
                    "payload": payload,
                }

                # Deterministic deduplication: keep the earliest source row.
                if (
                    item_id not in valid_items
                    or row_number < valid_items[item_id]["row_number"]
                ):
                    valid_items[item_id] = candidate

            # Rebuild this output from the current Bronze snapshot.
            cur.execute("DELETE FROM silver.order_items")
            cur.execute(
                """
                DELETE FROM silver.rejected_records
                WHERE source_file = %s
                """,
                (ITEMS_FILE,),
            )

            values = []

            for item_id, item in valid_items.items():
                payload = item["payload"]

                values.append(
                    (
                        item_id,
                        payload["order_id"],
                        payload["product_id"],
                        int(payload["quantity"]),
                        Decimal(str(payload["unit_price"])),
                        Decimal(str(payload.get("item_discount_amount", 0))),
                        item["row_number"],
                        item["source_id"],
                        run_id,
                    )
                )

            if values:
                execute_values(
                    cur,
                    """
                    INSERT INTO silver.order_items (
                        order_item_id,
                        order_id,
                        product_id,
                        quantity,
                        unit_price,
                        item_discount_amount,
                        source_record_number,
                        source_record_id,
                        pipeline_run_id
                    ) VALUES %s
                    """,
                    values,
                )

            if rejected_items:
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
                    [
                        (
                            source_file,
                            row_number,
                            source_id,
                            payload.get("order_id"),
                            reason,
                            payload_json,
                            item_run_id,
                        )
                        for (
                            source_file,
                            row_number,
                            source_id,
                            _item_id,
                            reason,
                            payload_json,
                            item_run_id,
                        ) in rejected_items
                    ],
                )

            print("Silver order items pipeline completed.")
            print(f"Pipeline run ID: {run_id}")
            print(f"Bronze records read: {len(source_rows)}")
            print(f"Accepted unique items: {len(valid_items)}")
            print(f"Rejected records: {len(rejected_items)}")


if __name__ == "__main__":
    main()
