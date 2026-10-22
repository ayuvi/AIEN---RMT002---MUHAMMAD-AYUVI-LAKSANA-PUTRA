import os
import sys
import uuid

import psycopg2


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "connect_timeout": 10,
    "application_name": "build_executive_kpis_daily",
}

RUN_ID = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())


def main():
    conn = None

    try:
        print("STEP 1/5: Connecting to PostgreSQL...", flush=True)
        conn = psycopg2.connect(**DB_CONFIG)

        with conn:
            with conn.cursor() as cur:
                cur.execute("SET LOCAL TIME ZONE 'UTC'")
                cur.execute("SET LOCAL statement_timeout = '180s'")
                cur.execute("SET LOCAL lock_timeout = '15s'")

                print("STEP 2/5: Checking required tables...", flush=True)

                required_tables = [
                    "gold.order_360",
                    "gold.customer_daily",
                    "gold.product_daily",
                    "gold.executive_kpis_daily",
                    "silver.payment_events",
                    "silver.refund_events",
                    "silver.return_events",
                    "silver.support_events",
                ]

                for table in required_tables:
                    cur.execute("SELECT to_regclass(%s)", (table,))
                    if cur.fetchone()[0] is None:
                        raise RuntimeError(
                            f"Required table does not exist: {table}"
                        )

                print("All required tables exist.", flush=True)
                print("STEP 3/5: Building daily executive KPIs...", flush=True)

                sql = """
                WITH date_bounds AS (
                    SELECT
                        MIN(order_date) AS min_date,
                        MAX(order_date) AS max_date
                    FROM gold.order_360
                ),

                calendar AS (
                    SELECT gs::date AS metric_date
                    FROM date_bounds b
                    CROSS JOIN LATERAL generate_series(
                        b.min_date::timestamp,
                        b.max_date::timestamp,
                        INTERVAL '1 day'
                    ) AS gs
                ),

                order_metrics AS (
                    SELECT
                        order_date AS metric_date,
                        COUNT(*)::integer AS total_orders,
                        COALESCE(
                            SUM(gross_merchandise_value), 0
                        )::numeric(14,2) AS gross_revenue,
                        COALESCE(
                            SUM(net_revenue), 0
                        )::numeric(14,2) AS net_revenue,
                        CASE
                            WHEN COUNT(*) > 0
                            THEN SUM(net_revenue) / COUNT(*)
                            ELSE 0
                        END::numeric(14,2) AS average_order_value,
                        COUNT(DISTINCT customer_id)::integer
                            AS active_customers
                    FROM gold.order_360
                    GROUP BY order_date
                ),

                captured_orders AS (
                    SELECT
                        (p.occurred_at_utc AT TIME ZONE 'UTC')::date
                            AS metric_date,
                        COUNT(DISTINCT p.order_id)::integer
                            AS captured_order_count
                    FROM silver.payment_events p
                    WHERE p.event_type = 'PAYMENT_CAPTURED'
                      AND p.order_id IS NOT NULL
                    GROUP BY
                        (p.occurred_at_utc AT TIME ZONE 'UTC')::date
                ),

                refunded_orders AS (
                    SELECT
                        (r.occurred_at_utc AT TIME ZONE 'UTC')::date
                            AS metric_date,
                        COUNT(DISTINCT r.order_id)::integer
                            AS refunded_order_count
                    FROM silver.refund_events r
                    WHERE r.event_type = 'REFUND_COMPLETED'
                      AND r.order_id IS NOT NULL
                    GROUP BY
                        (r.occurred_at_utc AT TIME ZONE 'UTC')::date
                ),

                returned_orders AS (
                    SELECT
                        o.order_date AS metric_date,
                        COUNT(DISTINCT o.order_id)::integer
                            AS return_order_count
                    FROM gold.order_360 o
                    WHERE o.return_status IN (
                        'RETURN_REQUESTED',
                        'RETURN_RECEIVED',
                        'RETURN_CLOSED'
                    )
                    GROUP BY o.order_date
                ),

                resolved_orders AS (
                    SELECT
                        order_date AS metric_date,
                        COUNT(*)::integer AS resolved_order_count
                    FROM gold.order_360
                    WHERE order_status IN (
                        'FULFILLED',
                        'RETURNED',
                        'CANCELLED'
                    )
                    GROUP BY order_date
                ),

                customer_metrics AS (
                    SELECT
                        metric_date,
                        COUNT(*) FILTER (
                            WHERE repeat_customer_flag
                        )::integer AS repeat_customers
                    FROM gold.customer_daily
                    GROUP BY metric_date
                ),

                inventory_metrics AS (
                    SELECT
                        metric_date,
                        COUNT(*) FILTER (
                            WHERE stockout_flag IS NOT NULL
                        )::integer AS inventory_products,
                        COUNT(*) FILTER (
                            WHERE stockout_flag IS TRUE
                        )::integer AS stockout_products
                    FROM gold.product_daily
                    GROUP BY metric_date
                ),

                support_metrics AS (
                    SELECT
                        (occurred_at_utc AT TIME ZONE 'UTC')::date
                            AS metric_date,
                        COUNT(DISTINCT ticket_id)::integer
                            AS support_contacts
                    FROM silver.support_events
                    GROUP BY
                        (occurred_at_utc AT TIME ZONE 'UTC')::date
                )

                INSERT INTO gold.executive_kpis_daily (
                    metric_date,
                    total_orders,
                    gross_revenue,
                    net_revenue,
                    average_order_value,
                    refund_rate,
                    return_rate,
                    repeat_customer_rate,
                    stockout_rate,
                    active_customers,
                    support_contact_rate,
                    data_freshness_utc,
                    pipeline_run_id
                )
                SELECT
                    c.metric_date,

                    COALESCE(o.total_orders, 0),
                    COALESCE(o.gross_revenue, 0),
                    COALESCE(o.net_revenue, 0),
                    COALESCE(o.average_order_value, 0),

                    CASE
                        WHEN COALESCE(cp.captured_order_count, 0) > 0
                        THEN ROUND(
                            COALESCE(rf.refunded_order_count, 0)::numeric
                            / cp.captured_order_count,
                            4
                        )
                        ELSE 0
                    END AS refund_rate,

                    CASE
                        WHEN COALESCE(ro.resolved_order_count, 0) > 0
                        THEN ROUND(
                            COALESCE(rt.return_order_count, 0)::numeric
                            / ro.resolved_order_count,
                            4
                        )
                        ELSE 0
                    END AS return_rate,

                    CASE
                        WHEN COALESCE(o.active_customers, 0) > 0
                        THEN ROUND(
                            COALESCE(cm.repeat_customers, 0)::numeric
                            / o.active_customers,
                            4
                        )
                        ELSE 0
                    END AS repeat_customer_rate,

                    CASE
                        WHEN COALESCE(im.inventory_products, 0) > 0
                        THEN ROUND(
                            im.stockout_products::numeric
                            / im.inventory_products,
                            4
                        )
                        ELSE 0
                    END AS stockout_rate,

                    COALESCE(o.active_customers, 0),

                    CASE
                        WHEN COALESCE(o.active_customers, 0) > 0
                        THEN ROUND(
                            COALESCE(sm.support_contacts, 0)::numeric
                            / o.active_customers,
                            4
                        )
                        ELSE 0
                    END AS support_contact_rate,

                    NOW(),
                    %s

                FROM calendar c
                LEFT JOIN order_metrics o
                    ON o.metric_date = c.metric_date
                LEFT JOIN captured_orders cp
                    ON cp.metric_date = c.metric_date
                LEFT JOIN refunded_orders rf
                    ON rf.metric_date = c.metric_date
                LEFT JOIN returned_orders rt
                    ON rt.metric_date = c.metric_date
                LEFT JOIN resolved_orders ro
                    ON ro.metric_date = c.metric_date
                LEFT JOIN customer_metrics cm
                    ON cm.metric_date = c.metric_date
                LEFT JOIN inventory_metrics im
                    ON im.metric_date = c.metric_date
                LEFT JOIN support_metrics sm
                    ON sm.metric_date = c.metric_date

                ON CONFLICT (metric_date)
                DO UPDATE SET
                    total_orders = EXCLUDED.total_orders,
                    gross_revenue = EXCLUDED.gross_revenue,
                    net_revenue = EXCLUDED.net_revenue,
                    average_order_value = EXCLUDED.average_order_value,
                    refund_rate = EXCLUDED.refund_rate,
                    return_rate = EXCLUDED.return_rate,
                    repeat_customer_rate = EXCLUDED.repeat_customer_rate,
                    stockout_rate = EXCLUDED.stockout_rate,
                    active_customers = EXCLUDED.active_customers,
                    support_contact_rate = EXCLUDED.support_contact_rate,
                    data_freshness_utc = EXCLUDED.data_freshness_utc,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """

                cur.execute(sql, (RUN_ID,))
                print(
                    f"Upsert completed. Rows affected: {cur.rowcount}",
                    flush=True,
                )

                print("STEP 4/5: Validating output...", flush=True)

                cur.execute("""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(DISTINCT metric_date) AS unique_dates,
                        MIN(metric_date) AS first_date,
                        MAX(metric_date) AS last_date,
                        COUNT(*) FILTER (
                            WHERE total_orders < 0
                               OR gross_revenue < 0
                               OR active_customers < 0
                        ) AS invalid_rows,
                        COUNT(*) FILTER (
                            WHERE refund_rate < 0 OR refund_rate > 1
                        ) AS invalid_refund_rates,
                        COUNT(*) FILTER (
                            WHERE return_rate < 0 OR return_rate > 1
                        ) AS invalid_return_rates
                    FROM gold.executive_kpis_daily
                """)

                (
                    total_rows,
                    unique_dates,
                    first_date,
                    last_date,
                    invalid_rows,
                    invalid_refund_rates,
                    invalid_return_rates,
                ) = cur.fetchone()

                if total_rows == 0:
                    raise RuntimeError(
                        "gold.executive_kpis_daily is empty."
                    )

                if total_rows != unique_dates:
                    raise RuntimeError(
                        "Duplicate metric_date values detected."
                    )

                if invalid_rows or invalid_refund_rates or invalid_return_rates:
                    raise RuntimeError(
                        "KPI validation failed: "
                        f"invalid_rows={invalid_rows}, "
                        f"invalid_refund_rates={invalid_refund_rates}, "
                        f"invalid_return_rates={invalid_return_rates}"
                    )

                print("STEP 5/5: Completed successfully.", flush=True)
                print(f"Pipeline run ID: {RUN_ID}")
                print(f"Rows: {total_rows}")
                print(f"Unique dates: {unique_dates}")
                print(f"Date range: {first_date} to {last_date}")
                print(f"Invalid rows: {invalid_rows}")
                print(f"Invalid refund rates: {invalid_refund_rates}")
                print(f"Invalid return rates: {invalid_return_rates}")

        print("Database transaction committed.", flush=True)

    except Exception as exc:
        if conn is not None:
            conn.rollback()

        print(
            f"ERROR: build_executive_kpis_daily.py failed: {exc}",
            file=sys.stderr,
            flush=True,
        )
        raise

    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.", flush=True)


if __name__ == "__main__":
    main()
