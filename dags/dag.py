from __future__ import annotations

import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_DIR = Path("/opt/airflow/project")

DB_CONFIG = {
    "host": os.getenv("PGHOST", "host.docker.internal"),
    "port": int(os.getenv("PGPORT", "5433")),
    "dbname": os.getenv("PGDATABASE", "retail_analytics"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "postgres"),
}


def get_run_id(context):
    return context["ti"].xcom_pull(task_ids="start_pipeline")


def start_pipeline(**context):
    run_id = str(uuid.uuid4())
    schema_file = PROJECT_DIR / "database/schemas/ops.sql"

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(schema_file.read_text())
            cur.execute(
                """
                INSERT INTO ops.pipeline_runs
                    (pipeline_run_id, dag_id, status, started_at)
                VALUES (%s, %s, 'RUNNING', NOW())
                ON CONFLICT (pipeline_run_id) DO NOTHING
                """,
                (run_id, context["dag"].dag_id),
            )

    return run_id


def validate_raw_data(**context):
    raw_dir = PROJECT_DIR / "data/raw"
    files = list(raw_dir.rglob("*.json")) + list(raw_dir.rglob("*.csv"))

    if not files:
        raise ValueError(f"Tidak ada file CSV/JSON di {raw_dir}")

    print(f"Raw data validation passed: {len(files)} files found.")


def run_script(script_path, **context):
    script = PROJECT_DIR / script_path
    if not script.is_file():
        raise FileNotFoundError(f"Script tidak ditemukan: {script}")

    env = os.environ.copy()
    env.update({key: str(value) for key, value in DB_CONFIG.items()})
    env["PGDATABASE"] = DB_CONFIG["dbname"]
    env["PGUSER"] = DB_CONFIG["user"]
    env["PGPASSWORD"] = DB_CONFIG["password"]
    env["PGHOST"] = DB_CONFIG["host"]
    env["PGPORT"] = str(DB_CONFIG["port"])
    env["PIPELINE_RUN_ID"] = get_run_id(context)

    subprocess.run(
        [sys.executable, str(script)],
        cwd=str(PROJECT_DIR),
        env=env,
        check=True,
    )


def quality_gate(**context):
    run_id = get_run_id(context)

    silver_tables = [
        "orders",
        "order_items",
        "payment_events",
        "refund_events",
        "return_events",
        "support_events",
        "web_events",
        "customers",
        "products",
        "promotions",
        "order_promotions",
        "inventory_snapshots",
        "campaign_spend",
    ]

    failures = []

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for table in silver_tables:
                check_name = f"silver_{table}_nonempty"

                cur.execute(
                    f"SELECT COUNT(*) FROM silver.{table}"
                )
                count = cur.fetchone()[0]
                passed = count > 0

                cur.execute(
                    """
                    INSERT INTO ops.quality_checks
                        (pipeline_run_id, check_name, status, details)
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (
                        run_id,
                        check_name,
                        "PASS" if passed else "FAIL",
                        f'{{"row_count": {count}}}',
                    ),
                )

                if not passed:
                    failures.append(
                        f"silver.{table}: row_count={count}"
                    )

    if failures:
        raise ValueError(
            "Silver quality gate failed: " + "; ".join(failures)
        )

    print(
        f"Silver quality gate passed for {len(silver_tables)} tables."
    )


def validate_gold(**context):
    run_id = get_run_id(context)

    gold_tables = [
        "order_360",
        "customer_daily",
        "product_daily",
        "channel_campaign_daily",
        "executive_kpis_daily",
    ]

    failures = []

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for table in gold_tables:
                check_name = f"gold_{table}_valid"

                cur.execute(
                    f"""
                    SELECT
                        COUNT(*),
                        COUNT(*) FILTER (
                            WHERE pipeline_run_id = %s
                        )
                    FROM gold.{table}
                    """,
                    (run_id,),
                )
                total_rows, current_run_rows = cur.fetchone()

                passed = total_rows > 0 and current_run_rows == total_rows

                cur.execute(
                    """
                    INSERT INTO ops.quality_checks
                        (pipeline_run_id, check_name, status, details)
                    VALUES (%s, %s, %s, %s::jsonb)
                    """,
                    (
                        run_id,
                        check_name,
                        "PASS" if passed else "FAIL",
                        (
                            f'{{"row_count": {total_rows}, '
                            f'"current_run_rows": {current_run_rows}}}'
                        ),
                    ),
                )

                if not passed:
                    failures.append(
                        f"gold.{table}: total={total_rows}, "
                        f"current_run={current_run_rows}"
                    )

    if failures:
        raise ValueError(
            "Gold validation failed: " + "; ".join(failures)
        )

    print(
        f"Gold validation passed for {len(gold_tables)} tables."
    )


def finish_pipeline(**context):
    run_id = get_run_id(context)

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE ops.pipeline_runs
                SET status = 'SUCCESS', finished_at = NOW()
                WHERE pipeline_run_id = %s
                """,
                (run_id,),
            )


def fail_pipeline(context):
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="start_pipeline")
    if not run_id:
        return

    error = str(context.get("exception", "Task failed"))[:4000]

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE ops.pipeline_runs
                    SET status = 'FAILED',
                        finished_at = NOW(),
                        error_message = %s
                    WHERE pipeline_run_id = %s
                    """,
                    (error, run_id),
                )
    except Exception as exc:
        print(f"Gagal mencatat status pipeline: {exc}")


with DAG(
    dag_id="omnichannel_retail_pipeline",
    description="Bronze to Silver to Gold retail analytics pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args={"on_failure_callback": fail_pipeline},
    tags=["milestone1", "retail", "etl"],
) as dag:
    start = PythonOperator(
        task_id="start_pipeline",
        python_callable=start_pipeline,
    )

    validate_raw = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data,
    )

    bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/bronze/build_bronze.py"},
    )

    silver_orders = PythonOperator(
        task_id="build_silver_orders",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/silver/build_silver.py"},
    )

    silver_items = PythonOperator(
        task_id="build_silver_items",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/silver/build_silver_items.py"},
    )

    silver_events = PythonOperator(
        task_id="build_silver_events",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/silver/build_silver_events.py"},
    )

    silver_reference = PythonOperator(
        task_id="build_silver_reference",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/silver/build_silver_reference.py"},
    )

    silver_web = PythonOperator(
        task_id="build_silver_web",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/silver/build_silver_web.py"},
    )

    gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate,
    )

    gold_order = PythonOperator(
        task_id="build_gold_order_360",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/gold/build_gold.py"},
    )

    gold_customer = PythonOperator(
        task_id="build_gold_customer_daily",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/gold/build_customer_daily.py"},
    )

    gold_product = PythonOperator(
        task_id="build_gold_product_daily",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/gold/build_product_daily.py"},
    )

    gold_campaign = PythonOperator(
        task_id="build_gold_channel_campaign_daily",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/gold/build_channel_campaign_daily.py"},
    )

    gold_kpis = PythonOperator(
        task_id="build_gold_executive_kpis_daily",
        python_callable=run_script,
        op_kwargs={"script_path": "pipelines/gold/build_executive_kpis_daily.py"},
    )

    validate_gold_task = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold,
    )

    finish = PythonOperator(
        task_id="finish_pipeline",
        python_callable=finish_pipeline,
    )

    start >> validate_raw >> bronze
    bronze >> [silver_orders, silver_items, silver_events, silver_reference, silver_web]
    [silver_orders, silver_items, silver_events, silver_reference, silver_web] >> gate
    gate >> gold_order >> gold_customer >> gold_product >> gold_campaign >> gold_kpis
    gold_kpis >> validate_gold_task >> finish