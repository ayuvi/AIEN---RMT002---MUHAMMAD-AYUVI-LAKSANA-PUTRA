
import os
import sys
import traceback
from datetime import datetime

import psycopg2


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "connect_timeout": 10,
    "application_name": "build_product_daily",
}


def log(message):
    print(message, flush=True)


def main():
    run_id = os.getenv("PIPELINE_RUN_ID") or datetime.now().astimezone().strftime(
        "product-daily-%Y%m%dT%H%M%S%z"
    )
    conn = None

    try:
        log("STEP 1/7: Connecting to PostgreSQL...")
        conn = psycopg2.connect(**DB_CONFIG)

        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL statement_timeout = '120s'")
                cur.execute("SET LOCAL lock_timeout = '10s'")

                log("STEP 2/7: Preparing product sales...")
                cur.execute("""
                    CREATE TEMP TABLE tmp_product_sales
                    ON COMMIT DROP AS
                    WITH item_product AS (
                        SELECT
                            order_id,
                            product_id,
                            SUM(quantity)::integer AS units_sold,
                            SUM(quantity * unit_price) AS gross_revenue
                        FROM silver.order_items
                        GROUP BY order_id, product_id
                    ),
                    item_totals AS (
                        SELECT
                            order_id,
                            SUM(gross_revenue) AS order_gmv
                        FROM item_product
                        GROUP BY order_id
                    )
                    SELECT
                        o.order_date AS metric_date,
                        i.product_id,
                        SUM(i.units_sold)::integer AS units_sold,
                        COUNT(DISTINCT i.order_id)::integer AS order_count,
                        SUM(i.gross_revenue)::numeric(14,2)
                            AS gross_revenue,
                        SUM(
                            CASE
                                WHEN t.order_gmv > 0 THEN
                                    o.net_revenue
                                    * i.gross_revenue / t.order_gmv
                                ELSE 0
                            END
                        )::numeric(14,2) AS net_revenue
                    FROM item_product i
                    JOIN item_totals t USING (order_id)
                    JOIN gold.order_360 o USING (order_id)
                    GROUP BY o.order_date, i.product_id
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX ON tmp_product_sales
                    (product_id, metric_date)
                """)
                log("Sales aggregation completed.")

                log("STEP 3/7: Preparing inventory...")
                cur.execute("""
                    CREATE TEMP TABLE tmp_product_inventory
                    ON COMMIT DROP AS
                    SELECT
                        snapshot_date AS metric_date,
                        product_id,
                        SUM(available_quantity)::integer
                            AS available_inventory,
                        SUM(reserved_quantity)::integer
                            AS reserved_inventory
                    FROM silver.inventory_snapshots
                    GROUP BY snapshot_date, product_id
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX ON tmp_product_inventory
                    (product_id, metric_date)
                """)
                log("Inventory aggregation completed.")

                log("STEP 4/7: Preparing promotions...")
                cur.execute("""
                    CREATE TEMP TABLE tmp_product_promotions
                    ON COMMIT DROP AS
                    SELECT
                        o.order_date AS metric_date,
                        i.product_id,
                        COUNT(DISTINCT p.promotion_id)::integer
                            AS promotion_count
                    FROM silver.order_items i
                    JOIN gold.order_360 o USING (order_id)
                    JOIN silver.order_promotions p USING (order_id)
                    GROUP BY o.order_date, i.product_id
                """)
                cur.execute("""
                    CREATE UNIQUE INDEX ON tmp_product_promotions
                    (product_id, metric_date)
                """)
                log("Promotion aggregation completed.")

                log("STEP 5/7: Preparing product categories...")
                cur.execute("""
                    CREATE TEMP TABLE tmp_product_categories
                    ON COMMIT DROP AS
                    SELECT
                        payload->>'product_id' AS product_id,
                        payload->>'category_name' AS category_name,
                        (payload->>'valid_from_utc')::timestamptz
                            AS valid_from,
                        (payload->>'valid_to_utc')::timestamptz
                            AS valid_to
                    FROM bronze.raw_records
                    WHERE position(
                        'product_categories' IN source_file
                    ) > 0
                      AND payload->>'product_id' IS NOT NULL
                      AND payload->>'category_name' IS NOT NULL
                      AND payload->>'valid_from_utc' IS NOT NULL
                """)
                cur.execute("""
                    CREATE INDEX ON tmp_product_categories
                    (product_id, valid_from DESC)
                """)
                log("Category data prepared.")

                log("STEP 6/7: Building product_daily...")
                cur.execute("""
                    WITH activity AS (
                        SELECT product_id, metric_date
                        FROM tmp_product_sales
                        UNION
                        SELECT product_id, metric_date
                        FROM tmp_product_inventory
                    )
                    INSERT INTO gold.product_daily (
                        product_id,
                        metric_date,
                        active_category,
                        units_sold,
                        order_count,
                        gross_revenue,
                        net_revenue,
                        refunded_units,
                        available_inventory,
                        reserved_inventory,
                        stockout_flag,
                        promotion_count,
                        pipeline_run_id
                    )
                    SELECT
                        a.product_id,
                        a.metric_date,
                        c.category_name,
                        COALESCE(s.units_sold, 0),
                        COALESCE(s.order_count, 0),
                        COALESCE(s.gross_revenue, 0),
                        COALESCE(s.net_revenue, 0),
                        0,
                        i.available_inventory,
                        i.reserved_inventory,
                        CASE
                            WHEN i.product_id IS NULL THEN NULL
                            ELSE COALESCE(i.available_inventory, 0) <= 0
                        END,
                        COALESCE(p.promotion_count, 0),
                        %s
                    FROM activity a
                    LEFT JOIN tmp_product_sales s
                      ON s.product_id = a.product_id
                     AND s.metric_date = a.metric_date
                    LEFT JOIN tmp_product_inventory i
                      ON i.product_id = a.product_id
                     AND i.metric_date = a.metric_date
                    LEFT JOIN tmp_product_promotions p
                      ON p.product_id = a.product_id
                     AND p.metric_date = a.metric_date
                    LEFT JOIN LATERAL (
                        SELECT tc.category_name
                        FROM tmp_product_categories tc
                        WHERE tc.product_id = a.product_id
                          AND a.metric_date >=
                              (tc.valid_from AT TIME ZONE 'UTC')::date
                          AND (
                              tc.valid_to IS NULL
                              OR a.metric_date <=
                                 (tc.valid_to AT TIME ZONE 'UTC')::date
                          )
                        ORDER BY tc.valid_from DESC
                        LIMIT 1
                    ) c ON TRUE
                    ON CONFLICT (product_id, metric_date) DO UPDATE SET
                        active_category = EXCLUDED.active_category,
                        units_sold = EXCLUDED.units_sold,
                        order_count = EXCLUDED.order_count,
                        gross_revenue = EXCLUDED.gross_revenue,
                        net_revenue = EXCLUDED.net_revenue,
                        refunded_units = EXCLUDED.refunded_units,
                        available_inventory = EXCLUDED.available_inventory,
                        reserved_inventory = EXCLUDED.reserved_inventory,
                        stockout_flag = EXCLUDED.stockout_flag,
                        promotion_count = EXCLUDED.promotion_count,
                        pipeline_run_id = EXCLUDED.pipeline_run_id
                """, (run_id,))

                log("Insert/upsert completed.")

                log("STEP 7/7: Validating results...")
                cur.execute("""
                    SELECT
                        COUNT(*),
                        COUNT(DISTINCT (product_id, metric_date)),
                        MIN(metric_date),
                        MAX(metric_date),
                        COUNT(*) FILTER (
                            WHERE active_category IS NULL
                        ),
                        COUNT(*) FILTER (
                            WHERE available_inventory IS NULL
                        )
                    FROM gold.product_daily
                """)
                (
                    total,
                    unique_grain,
                    first_date,
                    last_date,
                    missing_category,
                    missing_inventory,
                ) = cur.fetchone()

        log("")
        log("Gold product_daily load completed.")
        log(f"Rows: {total}")
        log(f"Unique product/date grain: {unique_grain}")
        log(f"Date range: {first_date} to {last_date}")
        log(f"Missing categories: {missing_category}")
        log(f"Missing inventory: {missing_inventory}")

        if total != unique_grain:
            raise RuntimeError("Duplicate product/date grain detected.")

    except Exception:
        log("ERROR: build_product_daily.py failed.")
        traceback.print_exc(file=sys.stdout)
        raise

    finally:
        if conn is not None:
            conn.close()
            log("Database connection closed.")


if __name__ == "__main__":
    main()
