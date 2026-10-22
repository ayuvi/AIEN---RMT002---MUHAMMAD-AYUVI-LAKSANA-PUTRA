import os
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values


conn = psycopg2.connect(
    host=os.getenv("PGHOST", "127.0.0.1"),
    port=int(os.getenv("PGPORT", "5433")),
    dbname=os.getenv("PGDATABASE", "retail_analytics"),
    user=os.getenv("PGUSER", "postgres"),
    password=os.getenv("PGPASSWORD", "postgres"),
)

try:
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT source_record_number, payload, pipeline_run_id
                FROM bronze.raw_records
                WHERE source_file = 'events/web_events.json'
                ORDER BY source_record_number
            """)
            records = cur.fetchall()

            accepted = {}
            rejected = 0

            for record_number, payload, bronze_run_id in records:
                try:
                    event_id = payload["event_id"]
                    event_type = payload["event_type"]
                    occurred = datetime.fromisoformat(
                        payload["occurred_at_utc"].replace("Z", "+00:00")
                    )
                    ingested = datetime.fromisoformat(
                        payload["ingested_at_utc"].replace("Z", "+00:00")
                    )
                    if occurred.tzinfo is None or ingested.tzinfo is None:
                        raise ValueError("Timestamp requires timezone")

                    details = payload.get("payload") or {}
                    if not event_id or not event_type:
                        raise ValueError("Missing event identity")

                    row = (
                        event_id, event_type, occurred, ingested,
                        details.get("session_id"),
                        details.get("order_id"),
                        details.get("customer_id"),
                        details.get("channel"),
                        details.get("campaign_id"),
                        bronze_run_id or "silver-web",
                    )
                    accepted.setdefault(event_id, row)

                except (KeyError, TypeError, ValueError, AttributeError):
                    rejected += 1

            if accepted:
                sql = """
                    INSERT INTO silver.web_events (
                        event_id, event_type, occurred_at_utc,
                        ingested_at_utc, session_id, order_id,
                        customer_id, channel, campaign_id, pipeline_run_id
                    )
                    VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET
                        event_type = EXCLUDED.event_type,
                        occurred_at_utc = EXCLUDED.occurred_at_utc,
                        ingested_at_utc = EXCLUDED.ingested_at_utc,
                        session_id = EXCLUDED.session_id,
                        order_id = EXCLUDED.order_id,
                        customer_id = EXCLUDED.customer_id,
                        channel = EXCLUDED.channel,
                        campaign_id = EXCLUDED.campaign_id,
                        pipeline_run_id = EXCLUDED.pipeline_run_id
                """
                execute_values(cur, sql, list(accepted.values()), page_size=1000)

            print("Bronze records read:", len(records))
            print("Unique valid events:", len(accepted))
            print("Rejected records:", rejected)
            print("Silver web events load completed.")

finally:
    conn.close()
