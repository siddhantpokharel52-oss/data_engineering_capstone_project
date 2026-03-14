"""Bronze layer loader — Read CSV files and bulk upload to Snowflake.

Root cause fix: Snowflake executemany() does not support PARSE_JSON()
as a bound expression in VALUES clause.

Solution: Insert JSON as plain VARCHAR into a temp table, then
SELECT PARSE_JSON(raw_data) into the Bronze VARIANT table.
This is fully supported and fast.
"""

import csv
import json
from pathlib import Path

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DB = "MBUST__025_MDS__07"
DATA_DIR = Path("/opt/airflow/data")

FILE_TABLE_MAP = {
    "customers": f"{DB}.BRONZE.RAW_CUSTOMERS",
    "products": f"{DB}.BRONZE.RAW_PRODUCTS",
    "orders": f"{DB}.BRONZE.RAW_ORDERS",
}


def load_bronze(**context):
    """Read CSV files and bulk load into Snowflake Bronze as VARIANT JSON.

    For each CSV file:
        1. Truncate the Bronze table (idempotency).
        2. Read CSV into memory.
        3. Insert JSON strings into a temp VARCHAR staging table.
        4. INSERT INTO Bronze SELECT PARSE_JSON(raw_json) FROM temp table.

    Args:
        **context: Airflow task context dictionary.

    Returns:
        None
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        for name, table in FILE_TABLE_MAP.items():
            csv_path = DATA_DIR / f"{name}.csv"

            if not csv_path.exists():
                raise FileNotFoundError(
                    f"CSV not found: {csv_path}. "
                    f"Ensure extract task ran successfully."
                )

            # Step 1: Read CSV
            with open(csv_path, newline="", buffering=8192) as f:
                rows = list(csv.DictReader(f))
            print(f"  Read {len(rows):,} rows from {csv_path.name}")

            # Step 2: Truncate Bronze table for idempotency
            cursor.execute(f"TRUNCATE TABLE {table}")

            # Step 3: Create a temp VARCHAR table to stage raw JSON strings
            temp_table = f"{table}_TEMP_LOAD"
            cursor.execute(
                f"""
                CREATE OR REPLACE TEMPORARY TABLE {temp_table} (
                    raw_json    VARCHAR,
                    source_file VARCHAR
                )
            """
            )

            # Step 4: Bulk insert JSON strings using executemany()
            # This works because we are inserting plain strings — no functions
            params = [(json.dumps(row), f"{name}.csv") for row in rows]
            cursor.executemany(
                f"INSERT INTO {temp_table} (raw_json, source_file) VALUES (%s, %s)",
                params,
            )
            print(f"  Staged {len(rows):,} rows in temp table")

            # Step 5: INSERT INTO Bronze using PARSE_JSON in a SELECT
            # PARSE_JSON is valid in SELECT — not in VALUES
            cursor.execute(
                f"""
                INSERT INTO {table} (raw_data, source_file)
                SELECT PARSE_JSON(raw_json), source_file
                FROM   {temp_table}
            """
            )

            # Step 6: Drop temp table
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

            conn.commit()
            print(f"  Inserted {len(rows):,} rows → {table}")

        print("Bronze load complete.")

    finally:
        cursor.close()
        conn.close()
