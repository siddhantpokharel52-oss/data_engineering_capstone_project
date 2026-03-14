"""Success report task — Daily Summary Report via Snowflake Alert.

Requirement: On successful pipeline run, generate a Daily Summary Report
delivered to the stakeholder via Snowflake Alert.

How it works:
1. Queries Gold layer for key business metrics
2. Builds a summary string
3. Calls SYSTEM$SEND_EMAIL() via Snowflake Alert
   — stakeholder receives email from Snowflake directly
   — uses Snowflake Notification Integration (no SMTP needed)
"""

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DB = "MBUST__025_MDS__07"


def success_report(**context):
    """Trigger the Snowflake Alert to send the Daily Summary Report.

    Queries Gold layer metrics, then manually fires the Snowflake Alert
    using EXECUTE ALERT so the report is sent immediately after a
    successful pipeline run rather than waiting for the cron schedule.

    Args:
        **context: Airflow task context dictionary.

    Returns:
        None
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    execution_date = context["ds"]

    # Step 1: Gather metrics from Gold layer
    overall = hook.get_first(
        f"""
        SELECT
            COUNT(*)                      AS total_orders,
            ROUND(SUM(total_amount), 2)   AS total_revenue,
            COUNT(DISTINCT customer_sk)   AS unique_customers,
            COUNT(DISTINCT product_sk)    AS unique_products
        FROM {DB}.GOLD.FACT_ORDERS
    """
    )

    status_rows = hook.get_records(
        f"""
        SELECT status, COUNT(*) AS cnt, ROUND(SUM(total_amount),2) AS rev
        FROM   {DB}.GOLD.FACT_ORDERS
        GROUP  BY status
        ORDER  BY cnt DESC
    """
    )

    top_category = hook.get_first(
        f"""
        SELECT p.category, ROUND(SUM(f.total_amount),2) AS rev
        FROM   {DB}.GOLD.FACT_ORDERS  f
        JOIN   {DB}.GOLD.DIM_PRODUCT  p ON p.product_sk = f.product_sk
        GROUP  BY p.category
        ORDER  BY rev DESC
        LIMIT  1
    """
    )

    total_orders = overall[0] or 0
    total_revenue = overall[1] or 0.0
    unique_customers = overall[2] or 0
    unique_products = overall[3] or 0
    best_category = top_category[0] if top_category else "N/A"
    best_cat_rev = top_category[1] if top_category else 0.0

    # Build status summary line
    status_lines = " | ".join(
        f"{r[0]}: {r[1]:,} orders (INR {r[2]:,.2f})" for r in status_rows
    )

    # Step 2: Update the summary table so Snowflake Alert can read it
    # The Alert uses a SELECT on this table as its condition
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {DB}.GOLD.PIPELINE_SUMMARY (
            run_date         DATE,
            total_orders     NUMBER,
            total_revenue    FLOAT,
            unique_customers NUMBER,
            unique_products  NUMBER,
            top_category     STRING,
            top_category_rev FLOAT,
            status_breakdown STRING,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
    """
    )

    hook.run(
        f"""
        DELETE FROM {DB}.GOLD.PIPELINE_SUMMARY
        WHERE run_date = '{execution_date}'
    """
    )

    hook.run(
        f"""
        INSERT INTO {DB}.GOLD.PIPELINE_SUMMARY
            (run_date, total_orders, total_revenue,
             unique_customers, unique_products,
             top_category, top_category_rev, status_breakdown)
        VALUES (
            '{execution_date}',
            {total_orders},
            {total_revenue},
            {unique_customers},
            {unique_products},
            '{best_category}',
            {best_cat_rev},
            '{status_lines}'
        )
    """
    )

    # Step 3: Manually execute the Snowflake Alert to fire immediately
    try:
        hook.run(f"EXECUTE ALERT {DB}.GOLD.CAPSTONE_DAILY_ALERT")
        print("Snowflake Alert fired successfully.")
    except Exception as e:
        # Alert may not exist yet — remind user to run 05_alerts.sql
        print(
            f"Warning: Could not fire alert — make sure 05_alerts.sql was run. Error: {e}"
        )

    print(f"Success report task complete for {execution_date}")
    print(f"  Total orders    : {total_orders:,}")
    print(f"  Total revenue   : INR {total_revenue:,.2f}")
    print(f"  Unique customers: {unique_customers:,}")
    print(f"  Top category    : {best_category} (INR {best_cat_rev:,.2f})")
