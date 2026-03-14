"""Gold layer transformer — Build Star Schema with SCD Type 2.

Loads Silver data into the Gold Star Schema:
1. DIM_PRODUCT  — simple upsert via MERGE
2. DIM_CUSTOMER — SCD Type 2 (expire old → insert new version)
3. DIM_LOCATION — upsert distinct cities
4. DIM_DATE     — populate date dimension for all order dates
5. FACT_ORDERS  — idempotent DELETE + INSERT
"""

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DB = "MBUST__025_MDS__07"


def transform_gold(**context):
    """Execute all Gold layer transformations in dependency order.

    Args:
        **context: Airflow task context dictionary.

    Returns:
        None
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    _load_dim_product(hook)
    _load_dim_customer_scd2(hook)
    _load_dim_location(hook)
    _load_dim_date(hook)
    _load_fact_orders(hook)

    print("Gold transformation complete.")


# ── Dimension loaders ─────────────────────────────────────────────────────────


def _load_dim_product(hook):
    """Upsert products into DIM_PRODUCT using MERGE.

    Args:
        hook: Active SnowflakeHook instance.

    Returns:
        None
    """
    hook.run(
        f"""
        MERGE INTO {DB}.GOLD.DIM_PRODUCT tgt
        USING {DB}.SILVER.STG_PRODUCTS src
            ON tgt.product_id = src.product_id
        WHEN MATCHED THEN UPDATE SET
            product_name = src.product_name,
            category     = src.category,
            price        = src.price
        WHEN NOT MATCHED THEN INSERT
            (product_id, product_name, category, price)
        VALUES
            (src.product_id, src.product_name, src.category, src.price)
    """
    )
    print("  DIM_PRODUCT loaded")


def _load_dim_customer_scd2(hook):
    """Apply SCD Type 2 logic to DIM_CUSTOMER.

    Step 1: Expire old records where email or city has changed.
    Step 2: Insert new version rows for customers without a current record.

    Args:
        hook: Active SnowflakeHook instance.

    Returns:
        None
    """
    # Step 1: Expire changed records
    hook.run(
        f"""
        MERGE INTO {DB}.GOLD.DIM_CUSTOMER tgt
        USING {DB}.SILVER.STG_CUSTOMERS src
            ON  tgt.customer_id = src.customer_id
            AND tgt.is_current  = TRUE
        WHEN MATCHED AND (
            tgt.email <> src.email OR tgt.city <> src.city
        ) THEN UPDATE SET
            tgt.effective_end_date = DATEADD(day, -1, CURRENT_DATE()),
            tgt.is_current         = FALSE
    """
    )

    # Step 2: Insert new/updated versions
    hook.run(
        f"""
        INSERT INTO {DB}.GOLD.DIM_CUSTOMER
            (customer_id, customer_name, email, city,
             effective_start_date, effective_end_date, is_current)
        SELECT
            src.customer_id,
            src.name,
            src.email,
            src.city,
            CURRENT_DATE(),
            NULL,
            TRUE
        FROM {DB}.SILVER.STG_CUSTOMERS src
        WHERE NOT EXISTS (
            SELECT 1
            FROM   {DB}.GOLD.DIM_CUSTOMER d
            WHERE  d.customer_id = src.customer_id
              AND  d.is_current  = TRUE
        )
    """
    )
    print("  DIM_CUSTOMER loaded (SCD Type 2)")


def _load_dim_location(hook):
    """Upsert distinct cities into DIM_LOCATION.

    Args:
        hook: Active SnowflakeHook instance.

    Returns:
        None
    """
    hook.run(
        f"""
        MERGE INTO {DB}.GOLD.DIM_LOCATION tgt
        USING (
            SELECT DISTINCT city
            FROM   {DB}.SILVER.STG_CUSTOMERS
            WHERE  city IS NOT NULL
        ) src ON tgt.city = src.city
        WHEN NOT MATCHED THEN INSERT (city) VALUES (src.city)
    """
    )
    print("  DIM_LOCATION loaded")


def _load_dim_date(hook):
    """Populate DIM_DATE for all order dates present in Silver.

    Args:
        hook: Active SnowflakeHook instance.

    Returns:
        None
    """
    hook.run(
        f"""
        INSERT INTO {DB}.GOLD.DIM_DATE
            (date_sk, full_date, day_of_week, month_name, quarter, year)
        SELECT DISTINCT
            TO_NUMBER(TO_CHAR(order_date, 'YYYYMMDD')) AS date_sk,
            order_date                                 AS full_date,
            DAYNAME(order_date)                        AS day_of_week,
            MONTHNAME(order_date)                      AS month_name,
            QUARTER(order_date)                        AS quarter,
            YEAR(order_date)                           AS year
        FROM {DB}.SILVER.STG_ORDERS
        WHERE order_date IS NOT NULL
          AND TO_NUMBER(TO_CHAR(order_date, 'YYYYMMDD')) NOT IN (
              SELECT date_sk FROM {DB}.GOLD.DIM_DATE
          )
    """
    )
    print("  DIM_DATE populated")


def _load_fact_orders(hook):
    """Load orders into FACT_ORDERS using DELETE + INSERT for idempotency.

    Fix: replaced correlated subquery in JOIN with a direct JOIN to
    STG_CUSTOMERS — Snowflake does not support correlated subqueries
    inside JOIN ON clauses.

    Args:
        hook: Active SnowflakeHook instance.

    Returns:
        None
    """
    # Idempotency: remove existing rows for current orders
    hook.run(
        f"""
        DELETE FROM {DB}.GOLD.FACT_ORDERS
        WHERE order_id IN (
            SELECT order_id FROM {DB}.SILVER.STG_ORDERS
        )
    """
    )

    # INSERT with resolved surrogate keys
    # FIX: JOIN STG_CUSTOMERS directly instead of correlated subquery
    hook.run(
        f"""
        INSERT INTO {DB}.GOLD.FACT_ORDERS
            (order_id, customer_sk, product_sk, date_sk,
             location_sk, quantity, total_amount, status)
        SELECT
            o.order_id,
            c.customer_sk,
            p.product_sk,
            TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')) AS date_sk,
            l.location_sk,
            o.quantity,
            o.amount                                     AS total_amount,
            o.status
        FROM      {DB}.SILVER.STG_ORDERS    o
        JOIN      {DB}.GOLD.DIM_CUSTOMER    c  ON  c.customer_id = o.customer_id
                                               AND c.is_current  = TRUE
        JOIN      {DB}.GOLD.DIM_PRODUCT     p  ON  p.product_id  = o.product_id
        JOIN      {DB}.SILVER.STG_CUSTOMERS sc ON  sc.customer_id = o.customer_id
        JOIN      {DB}.GOLD.DIM_LOCATION    l  ON  l.city         = sc.city
        WHERE o.order_date IS NOT NULL
    """
    )
    print("  FACT_ORDERS loaded")
