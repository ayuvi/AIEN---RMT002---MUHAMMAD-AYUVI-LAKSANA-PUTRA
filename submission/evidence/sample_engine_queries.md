# Sample Engine Queries

Database: retail_analytics
Engine: PostgreSQL

## 1. Bronze source record count

SELECT
    source_file,
    COUNT(*) AS record_count
FROM bronze.raw_records
GROUP BY source_file
ORDER BY source_file;

## 2. Silver order quality overview

SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders
FROM silver.orders;

## 3. Gold order analytics

SELECT
    COUNT(*) AS total_orders,
    ROUND(SUM(gross_merchandise_value)::numeric, 2) AS total_gmv,
    ROUND(SUM(net_revenue)::numeric, 2) AS total_net_revenue,
    ROUND(SUM(refunded_amount)::numeric, 2) AS total_refunds
FROM gold.order_360;

## 4. Executive KPI summary

SELECT
    metric_date,
    total_orders,
    gross_revenue,
    net_revenue,
    average_order_value,
    refund_rate,
    return_rate,
    repeat_customer_rate,
    stockout_rate
FROM gold.executive_kpis_daily
ORDER BY metric_date DESC
LIMIT 10;

## 5. Pipeline execution status

SELECT
    pipeline_run_id,
    dag_id,
    status,
    started_at,
    finished_at,
    error_message
FROM ops.pipeline_runs
ORDER BY started_at DESC
LIMIT 10;

## 6. Quality check summary

SELECT
    pipeline_run_id,
    status,
    COUNT(*) AS total_checks
FROM ops.quality_checks
GROUP BY pipeline_run_id, status
ORDER BY pipeline_run_id, status;
