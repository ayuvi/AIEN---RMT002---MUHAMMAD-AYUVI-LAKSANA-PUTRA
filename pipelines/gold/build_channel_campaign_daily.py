import os
import sys
import uuid
from datetime import datetime, timezone

import psycopg2


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
    "connect_timeout": 10,
    "application_name": "build_channel_campaign_daily",
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

                print("STEP 2/5: Checking source tables...", flush=True)

                cur.execute("""
                    SELECT
                        to_regclass('silver.campaign_spend'),
                        to_regclass('silver.web_events'),
                        to_regclass('gold.order_360'),
                        to_regclass('gold.channel_campaign_daily')
                """)

                tables = cur.fetchone()

                if any(table is None for table in tables):
                    raise RuntimeError(
                        "One or more required tables do not exist: "
                        "silver.campaign_spend, silver.web_events, "
                        "gold.order_360, gold.channel_campaign_daily"
                    )

                print("All required tables exist.", flush=True)

                print("STEP 3/5: Building campaign metrics...", flush=True)

                sql = """
                WITH spend AS (
                    SELECT
                        spend_date AS metric_date,
                        channel AS sales_channel,
                        campaign_id,
                        SUM(spend_amount)::numeric(14,2)
                            AS campaign_spend
                    FROM silver.campaign_spend
                    GROUP BY spend_date, channel, campaign_id
                ),

                session_metrics AS (
                    SELECT
                        (occurred_at_utc AT TIME ZONE 'UTC')::date
                            AS metric_date,
                        channel AS sales_channel,
                        campaign_id,
                        COUNT(DISTINCT session_id)
                            AS session_count
                    FROM silver.web_events
                    WHERE campaign_id IS NOT NULL
                      AND channel IS NOT NULL
                      AND session_id IS NOT NULL
                    GROUP BY
                        (occurred_at_utc AT TIME ZONE 'UTC')::date,
                        channel,
                        campaign_id
                ),

                purchase_ranked AS (
                    SELECT
                        (occurred_at_utc AT TIME ZONE 'UTC')::date
                            AS metric_date,
                        channel AS sales_channel,
                        campaign_id,
                        order_id,
                        customer_id,
                        event_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY order_id
                            ORDER BY occurred_at_utc, event_id
                        ) AS order_rank
                    FROM silver.web_events
                    WHERE event_type = 'PURCHASE'
                      AND order_id IS NOT NULL
                      AND campaign_id IS NOT NULL
                      AND channel IS NOT NULL
                ),

                purchase_attribution AS (
                    SELECT
                        metric_date,
                        sales_channel,
                        campaign_id,
                        order_id,
                        customer_id
                    FROM purchase_ranked
                    WHERE order_rank = 1
                ),

                sales_metrics AS (
                    SELECT
                        p.metric_date,
                        p.sales_channel,
                        p.campaign_id,
                        COUNT(DISTINCT p.order_id)::integer
                            AS attributed_orders,
                        COUNT(DISTINCT p.customer_id)::integer
                            AS attributed_customers,
                        COALESCE(
                            SUM(o.gross_merchandise_value), 0
                        )::numeric(14,2) AS gross_revenue,
                        COALESCE(
                            SUM(o.net_revenue), 0
                        )::numeric(14,2) AS net_revenue,
                        COALESCE(
                            SUM(o.refunded_amount), 0
                        )::numeric(14,2) AS refunds
                    FROM purchase_attribution p
                    JOIN gold.order_360 o
                      ON o.order_id = p.order_id
                    GROUP BY
                        p.metric_date,
                        p.sales_channel,
                        p.campaign_id
                )

                INSERT INTO gold.channel_campaign_daily (
                    metric_date,
                    sales_channel,
                    campaign_id,
                    campaign_spend,
                    attributed_orders,
                    attributed_customers,
                    gross_revenue,
                    net_revenue,
                    refunds,
                    roas,
                    conversion_rate,
                    pipeline_run_id
                )
                SELECT
                    s.metric_date,
                    s.sales_channel,
                    s.campaign_id,
                    s.campaign_spend,
                    COALESCE(m.attributed_orders, 0),
                    COALESCE(m.attributed_customers, 0),
                    COALESCE(m.gross_revenue, 0),
                    COALESCE(m.net_revenue, 0),
                    COALESCE(m.refunds, 0),

                    CASE
                        WHEN s.campaign_spend > 0
                        THEN ROUND(
                            COALESCE(m.net_revenue, 0)
                            / s.campaign_spend,
                            4
                        )
                        ELSE NULL
                    END AS roas,

                    CASE
                        WHEN COALESCE(sm.session_count, 0) > 0
                        THEN ROUND(
                            COALESCE(m.attributed_orders, 0)::numeric
                            / sm.session_count,
                            4
                        )
                        ELSE NULL
                    END AS conversion_rate,

                    %s AS pipeline_run_id

                FROM spend s

                LEFT JOIN sales_metrics m
                  ON m.metric_date = s.metric_date
                 AND m.sales_channel = s.sales_channel
                 AND m.campaign_id = s.campaign_id

                LEFT JOIN session_metrics sm
                  ON sm.metric_date = s.metric_date
                 AND sm.sales_channel = s.sales_channel
                 AND sm.campaign_id = s.campaign_id

                ON CONFLICT (metric_date, sales_channel, campaign_id)
                DO UPDATE SET
                    campaign_spend = EXCLUDED.campaign_spend,
                    attributed_orders = EXCLUDED.attributed_orders,
                    attributed_customers = EXCLUDED.attributed_customers,
                    gross_revenue = EXCLUDED.gross_revenue,
                    net_revenue = EXCLUDED.net_revenue,
                    refunds = EXCLUDED.refunds,
                    roas = EXCLUDED.roas,
                    conversion_rate = EXCLUDED.conversion_rate,
                    pipeline_run_id = EXCLUDED.pipeline_run_id
                """

                cur.execute(sql, (RUN_ID,))
                print(
                    f"Upsert completed. Rows affected: {cur.rowcount}",
                    flush=True
                )

                print("STEP 4/5: Validating Gold output...", flush=True)

                cur.execute("""
                    SELECT
                        COUNT(*) AS total_rows,
                        COUNT(DISTINCT (
                            metric_date,
                            sales_channel,
                            campaign_id
                        )) AS unique_grain,
                        MIN(metric_date) AS first_date,
                        MAX(metric_date) AS last_date,
                        SUM(campaign_spend) AS total_spend,
                        SUM(attributed_orders) AS attributed_orders,
                        COUNT(*) FILTER (
                            WHERE roas < 0
                        ) AS negative_roas_rows
                    FROM gold.channel_campaign_daily
                """)

                result = cur.fetchone()
                (
                    total_rows,
                    unique_grain,
                    first_date,
                    last_date,
                    total_spend,
                    attributed_orders,
                    negative_roas_rows,
                ) = result

                if total_rows != unique_grain:
                    raise RuntimeError(
                        f"Grain validation failed: {total_rows} rows "
                        f"but {unique_grain} unique keys."
                    )

                if total_rows == 0:
                    raise RuntimeError(
                        "Gold campaign table is empty after loading."
                    )

                print("STEP 5/5: Completed successfully.", flush=True)
                print(f"Pipeline run ID: {RUN_ID}")
                print(f"Rows: {total_rows}")
                print(f"Unique grain: {unique_grain}")
                print(f"Date range: {first_date} to {last_date}")
                print(f"Total campaign spend: {total_spend}")
                print(f"Attributed orders: {attributed_orders}")
                print(f"Negative ROAS rows: {negative_roas_rows}")

        print("Database transaction committed.", flush=True)

    except Exception as exc:
        if conn is not None:
            conn.rollback()

        print(
            f"ERROR: build_channel_campaign_daily.py failed: {exc}",
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
