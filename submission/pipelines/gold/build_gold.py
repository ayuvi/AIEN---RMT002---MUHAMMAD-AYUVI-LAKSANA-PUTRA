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
        "gold-%Y%m%dT%H%M%S%z"
    )

    sql = """
    WITH
    item_agg AS (
        SELECT
            order_id,
            SUM(quantity * unit_price) AS gmv,
            SUM(item_discount_amount) AS item_discount,
            COUNT(*) AS item_count,
            SUM(quantity) AS unit_quantity
        FROM silver.order_items
        GROUP BY order_id
    ),
    promotion_agg AS (
        SELECT
            order_id,
            SUM(discount_amount) AS promotion_discount,
            COUNT(DISTINCT promotion_id) AS promotion_count
        FROM silver.order_promotions
        GROUP BY order_id
    ),
    payment_agg AS (
        SELECT
            order_id,
            SUM(amount) FILTER (
                WHERE event_type = 'PAYMENT_CAPTURED'
            ) AS captured_amount,
            BOOL_OR(event_type = 'PAYMENT_CAPTURED') AS has_capture,
            BOOL_OR(event_type = 'PAYMENT_AUTHORIZED') AS has_authorization,
            BOOL_OR(event_type = 'PAYMENT_FAILED') AS has_failure
        FROM silver.payment_events
        WHERE order_id IS NOT NULL
        GROUP BY order_id
    ),
    refund_agg AS (
        SELECT
            order_id,
            SUM(amount) FILTER (
                WHERE event_type = 'REFUND_COMPLETED'
            ) AS refunded_amount
        FROM silver.refund_events
        WHERE order_id IS NOT NULL
        GROUP BY order_id
    ),
    return_latest AS (
        SELECT DISTINCT ON (order_id)
            order_id,
            event_type AS return_status
        FROM silver.return_events
        WHERE order_id IS NOT NULL
        ORDER BY order_id, occurred_at_utc DESC, event_id DESC
    ),
    return_agg AS (
        SELECT
            order_id,
            BOOL_OR(event_type = 'RETURN_CLOSED') AS has_closed_return
        FROM silver.return_events
        WHERE order_id IS NOT NULL
        GROUP BY order_id
    ),
    order_base AS (
        SELECT
            o.order_id,
            o.customer_id,
            (o.ordered_at_utc AT TIME ZONE 'UTC')::date AS order_date,
            o.sales_channel,
            o.store_id,
            COALESCE(i.gmv, 0)::numeric(14,2) AS gmv,
            (
                COALESCE(i.item_discount, 0)
                + COALESCE(p.promotion_discount, 0)
            )::numeric(14,2) AS discount_amount,
            o.shipping_revenue::numeric(14,2) AS shipping_revenue,
            COALESCE(pay.captured_amount, 0)::numeric(14,2)
                AS captured_amount,
            COALESCE(ref.refunded_amount, 0)::numeric(14,2)
                AS refunded_amount,
            COALESCE(i.item_count, 0)::integer AS item_count,
            COALESCE(i.unit_quantity, 0)::integer AS unit_quantity,
            o.order_status,
            CASE
                WHEN COALESCE(pay.has_capture, FALSE)
                    THEN 'CAPTURED'
                WHEN COALESCE(pay.has_failure, FALSE)
                    THEN 'FAILED'
                WHEN COALESCE(pay.has_authorization, FALSE)
                    THEN 'AUTHORIZED'
                ELSE 'UNKNOWN'
            END AS payment_status,
            CASE
                 WHEN rl.return_status IS NOT NULL
                    THEN rl.return_status
                WHEN o.order_status = 'RETURNED'
                    THEN 'RETURNED'
                ELSE 'NO_RETURN'
            END AS return_status,
            COALESCE(p.promotion_count, 0)::integer AS promotion_count,
            o.ordered_at_utc,
            o.pipeline_run_id
        FROM silver.orders o
        LEFT JOIN item_agg i ON i.order_id = o.order_id
        LEFT JOIN promotion_agg p ON p.order_id = o.order_id
        LEFT JOIN payment_agg pay ON pay.order_id = o.order_id
        LEFT JOIN refund_agg ref ON ref.order_id = o.order_id
        LEFT JOIN return_latest rl ON rl.order_id = o.order_id
    ),
    final_order AS (
        SELECT
            b.*,
            COALESCE(ra.has_closed_return, FALSE) AS has_closed_return,
            (
                ROW_NUMBER() OVER (
                    PARTITION BY b.customer_id
                    ORDER BY b.ordered_at_utc, b.order_id
                ) = 1
            ) AS first_order_flag
        FROM order_base b
        LEFT JOIN return_agg ra ON ra.order_id = b.order_id
    )
    INSERT INTO gold.order_360 (
        order_id, customer_id, order_date, sales_channel, store_id,
        gross_merchandise_value, discount_amount, shipping_revenue,
        captured_payment_amount, refunded_amount, net_revenue,
        item_count, unit_quantity, order_status, payment_status,
        return_status, promotion_count, first_order_flag, pipeline_run_id
    )
    SELECT
        order_id,
        customer_id,
        order_date,
        sales_channel,
        store_id,
        gmv,
        discount_amount,
        shipping_revenue,
        captured_amount,
        refunded_amount,
        (
            gmv - discount_amount + shipping_revenue - refunded_amount
        )::numeric(14,2),
        item_count,
        unit_quantity,
        order_status,
        payment_status,
        return_status,
        promotion_count,
        first_order_flag,
        %s
    FROM final_order
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        order_date = EXCLUDED.order_date,
        sales_channel = EXCLUDED.sales_channel,
        store_id = EXCLUDED.store_id,
        gross_merchandise_value = EXCLUDED.gross_merchandise_value,
        discount_amount = EXCLUDED.discount_amount,
        shipping_revenue = EXCLUDED.shipping_revenue,
        captured_payment_amount = EXCLUDED.captured_payment_amount,
        refunded_amount = EXCLUDED.refunded_amount,
        net_revenue = EXCLUDED.net_revenue,
        item_count = EXCLUDED.item_count,
        unit_quantity = EXCLUDED.unit_quantity,
        order_status = EXCLUDED.order_status,
        payment_status = EXCLUDED.payment_status,
        return_status = EXCLUDED.return_status,
        promotion_count = EXCLUDED.promotion_count,
        first_order_flag = EXCLUDED.first_order_flag,
        pipeline_run_id = EXCLUDED.pipeline_run_id;
    """

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (run_id,))
                cur.execute("SELECT COUNT(*) FROM gold.order_360")
                total = cur.fetchone()[0]

                cur.execute("""
                    SELECT
                        COUNT(*) AS orders,
                        COALESCE(SUM(gross_merchandise_value), 0),
                        COALESCE(SUM(discount_amount), 0),
                        COALESCE(SUM(net_revenue), 0)
                    FROM gold.order_360
                """)
                summary = cur.fetchone()

        print("Gold order_360 load completed.")
        print("Rows in gold.order_360:", total)
        print("Orders / GMV / discounts / net revenue:", summary)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
