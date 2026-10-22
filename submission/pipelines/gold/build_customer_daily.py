
import os
from datetime import datetime

import psycopg2


DB_CONFIG = {
    "host": os.getenv("PGHOST", "127.0.0.1"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}


def main():
    run_id = os.getenv("PIPELINE_RUN_ID") or datetime.now().astimezone().strftime(
        "customer-daily-%Y%m%dT%H%M%S%z"
    )

    sql = """
    WITH
    order_metrics AS (
        SELECT
            order_date AS metric_date,
            customer_id,
            COUNT(*) AS order_count,
            SUM(unit_quantity) AS unit_quantity,
            SUM(gross_merchandise_value) AS gross_revenue,
            SUM(net_revenue) AS net_revenue,
            BOOL_OR(sales_channel IS NOT NULL) AS has_channel
        FROM gold.order_360
        GROUP BY order_date, customer_id
    ),
    refund_metrics AS (
        SELECT
            (r.occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            o.customer_id,
            SUM(r.amount) FILTER (
                WHERE r.event_type = 'REFUND_COMPLETED'
            ) AS refund_amount
        FROM silver.refund_events r
        JOIN silver.orders o
            ON o.order_id = r.order_id
        WHERE r.order_id IS NOT NULL
        GROUP BY
            (r.occurred_at_utc AT TIME ZONE 'UTC')::date,
            o.customer_id
    ),
    return_metrics AS (
        SELECT
            (r.occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            o.customer_id,
            COUNT(DISTINCT r.order_id) FILTER (
                WHERE r.event_type = 'RETURN_CLOSED'
            ) AS return_count
        FROM silver.return_events r
        JOIN silver.orders o
            ON o.order_id = r.order_id
        WHERE r.order_id IS NOT NULL
        GROUP BY
            (r.occurred_at_utc AT TIME ZONE 'UTC')::date,
            o.customer_id
    ),
    support_metrics AS (
        SELECT
            (s.occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
            s.customer_id,
            COUNT(DISTINCT s.ticket_id) AS support_contacts
        FROM silver.support_events s
        WHERE s.customer_id IS NOT NULL
        GROUP BY
            (s.occurred_at_utc AT TIME ZONE 'UTC')::date,
            s.customer_id
    ),
    activity AS (
        SELECT metric_date, customer_id FROM order_metrics
        UNION
        SELECT metric_date, customer_id FROM refund_metrics
        UNION
        SELECT metric_date, customer_id FROM return_metrics
        UNION
        SELECT metric_date, customer_id FROM support_metrics
    ),
    customer_first_order AS (
        SELECT
            customer_id,
            MIN(order_date) AS first_order_date
        FROM gold.order_360
        GROUP BY customer_id
    ),
    customer_channel AS (
        SELECT
            order_date AS metric_date,
            customer_id,
            (ARRAY_AGG(
                sales_channel
                ORDER BY ordered_at_utc DESC NULLS LAST
            ))[1] AS active_channel
        FROM (
            SELECT
                g.order_date,
                g.customer_id,
                g.sales_channel,
                o.ordered_at_utc
            FROM gold.order_360 g
            JOIN silver.orders o
                ON o.order_id = g.order_id
        ) x
        GROUP BY order_date, customer_id
    )
    INSERT INTO gold.customer_daily (
        customer_id, metric_date, order_count, unit_quantity,
        gross_revenue, net_revenue, refund_amount, return_count,
        support_contacts, active_channel, new_customer_flag,
        repeat_customer_flag, pipeline_run_id
    )
    SELECT
        a.customer_id,
        a.metric_date,
        COALESCE(om.order_count, 0)::integer,
        COALESCE(om.unit_quantity, 0)::integer,
        COALESCE(om.gross_revenue, 0)::numeric(14,2),
        COALESCE(om.net_revenue, 0)::numeric(14,2),
        COALESCE(rm.refund_amount, 0)::numeric(14,2),
        COALESCE(ret.return_count, 0)::integer,
        COALESCE(sm.support_contacts, 0)::integer,
        cc.active_channel,
        COALESCE(
            cfo.first_order_date = a.metric_date,
            FALSE
        ) AS new_customer_flag,
        COALESCE(
            cfo.first_order_date < a.metric_date,
            FALSE
        ) AS repeat_customer_flag,
        %s
    FROM activity a
    LEFT JOIN order_metrics om
        ON om.metric_date = a.metric_date
       AND om.customer_id = a.customer_id
    LEFT JOIN refund_metrics rm
        ON rm.metric_date = a.metric_date
       AND rm.customer_id = a.customer_id
    LEFT JOIN return_metrics ret
        ON ret.metric_date = a.metric_date
       AND ret.customer_id = a.customer_id
    LEFT JOIN support_metrics sm
        ON sm.metric_date = a.metric_date
       AND sm.customer_id = a.customer_id
    LEFT JOIN customer_first_order cfo
        ON cfo.customer_id = a.customer_id
    LEFT JOIN customer_channel cc
        ON cc.metric_date = a.metric_date
       AND cc.customer_id = a.customer_id
    ON CONFLICT (customer_id, metric_date) DO UPDATE SET
        order_count = EXCLUDED.order_count,
        unit_quantity = EXCLUDED.unit_quantity,
        gross_revenue = EXCLUDED.gross_revenue,
        net_revenue = EXCLUDED.net_revenue,
        refund_amount = EXCLUDED.refund_amount,
        return_count = EXCLUDED.return_count,
        support_contacts = EXCLUDED.support_contacts,
        active_channel = EXCLUDED.active_channel,
        new_customer_flag = EXCLUDED.new_customer_flag,
        repeat_customer_flag = EXCLUDED.repeat_customer_flag,
        pipeline_run_id = EXCLUDED.pipeline_run_id;
    """

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                cur.execute("SELECT COUNT(*) FROM gold.customer_daily")
                total = cur.fetchone()[0]

        print("Gold customer_daily load completed.")
        print("Rows in gold.customer_daily:", total)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
