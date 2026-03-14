# dags/tasks/transform_silver.py
"""Clean and type-cast Bronze data into Silver staging tables."""
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DB = "MBUST__025_MDS__07"


def transform_silver(**context):
    """Transform Bronze VARIANT rows into typed Silver tables.

    Uses INSERT OVERWRITE pattern for idempotency.
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # --- Customers ---
    hook.run(f"TRUNCATE TABLE {DB}.SILVER.STG_CUSTOMERS")
    hook.run(
        f"""
        INSERT INTO {DB}.SILVER.STG_CUSTOMERS
            (customer_id, name, email, city, signup_date)
        SELECT
            raw_data:customer_id::STRING,
            raw_data:name::STRING,
            raw_data:email::STRING,
            raw_data:city::STRING,
            raw_data:signup_date::DATE
        FROM {DB}.BRONZE.RAW_CUSTOMERS
        WHERE raw_data:customer_id IS NOT NULL
    """
    )

    # --- Products ---
    hook.run(f"TRUNCATE TABLE {DB}.SILVER.STG_PRODUCTS")
    hook.run(
        f"""
        INSERT INTO {DB}.SILVER.STG_PRODUCTS
            (product_id, product_name, category, price)
        SELECT
            raw_data:product_id::STRING,
            raw_data:product_name::STRING,
            raw_data:category::STRING,
            raw_data:price::FLOAT
        FROM {DB}.BRONZE.RAW_PRODUCTS
    """
    )

    # --- Orders --- (deduplicate by order_id)
    hook.run(f"TRUNCATE TABLE {DB}.SILVER.STG_ORDERS")
    hook.run(
        f"""
        INSERT INTO {DB}.SILVER.STG_ORDERS
            (order_id, customer_id, product_id, order_date,
             quantity, amount, status)
        SELECT DISTINCT
            raw_data:order_id::STRING,
            raw_data:customer_id::STRING,
            raw_data:product_id::STRING,
            raw_data:order_date::DATE,
            raw_data:quantity::INT,
            raw_data:amount::FLOAT,
            raw_data:status::STRING
        FROM {DB}.BRONZE.RAW_ORDERS
    """
    )
    print("Silver transformation complete.")
