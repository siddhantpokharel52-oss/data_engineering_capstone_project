"""Extract task — generate dummy data and save as CSV files.

Optimised version:
- Pre-computes date ranges once (avoids Faker re-parsing every call)
- Uses list comprehensions instead of loops where possible
- Writes all 3 CSV files to /opt/airflow/data/
"""

import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

from faker import Faker

fake = Faker("en_IN")

DB = "MBUST__025_MDS__07"

CITIES      = ["Hyderabad", "Mumbai", "Pune", "Bangalore",
                "Dehli", "Chennai", "Kolkata"]
CATEGORIES  = ["Electronics", "Clothing", "Books", "Home", "Sports"]
STATUSES    = ["completed", "pending", "cancelled"]

DATA_DIR    = Path("/opt/airflow/data")

# Pre-compute date ranges ONCE at module level — not inside every function call
_TODAY          = date.today()
_TWO_YEARS_AGO  = _TODAY - timedelta(days=730)
_ONE_YEAR_AGO   = _TODAY - timedelta(days=365)
_RANGE_2Y       = (_TODAY - _TWO_YEARS_AGO).days
_RANGE_1Y       = (_TODAY - _ONE_YEAR_AGO).days


def _rand_date(start: date, days_range: int) -> str:
    """Return a random date string without Faker date parsing overhead.

    Args:
        start: The earliest possible date.
        days_range: Number of days to randomly add from start.

    Returns:
        ISO date string (YYYY-MM-DD).
    """
    return str(start + timedelta(days=random.randint(0, days_range)))


def extract_data(**context):
    """Generate dummy CSVs and save to /opt/airflow/data/.

    Generates customers, products, and orders using Faker,
    then writes each dataset to a CSV file. Pushes row counts
    to XCom for downstream tasks.

    Args:
        **context: Airflow task context dictionary.

    Returns:
        None
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    customers = _generate_customers(300)
    products  = _generate_products(50)
    orders    = _generate_orders(customers, products, 1000)

    _write_csv(customers, DATA_DIR / "customers.csv")
    _write_csv(products,  DATA_DIR / "products.csv")
    _write_csv(orders,    DATA_DIR / "orders.csv")

    ti = context["ti"]
    ti.xcom_push(key="customer_count", value=len(customers))
    ti.xcom_push(key="product_count",  value=len(products))
    ti.xcom_push(key="order_count",    value=len(orders))

    print(
        f"Extract complete: {len(customers)} customers, "
        f"{len(products)} products, {len(orders)} orders "
        f"saved to {DATA_DIR}"
    )


# ── Generators ────────────────────────────────────────────────────────────────


def _generate_customers(n: int) -> list:
    """Generate n fake customer records.

    Args:
        n: Number of customers to generate.

    Returns:
        List of customer dicts.
    """
    return [
        {
            "customer_id": str(uuid.uuid4()),
            "name":        fake.name(),
            "email":       fake.email(),
            "city":        random.choice(CITIES),
            "signup_date": _rand_date(_TWO_YEARS_AGO, _RANGE_2Y),
        }
        for _ in range(n)
    ]


def _generate_products(n: int) -> list:
    """Generate n fake product records.

    Args:
        n: Number of products to generate.

    Returns:
        List of product dicts.
    """
    return [
        {
            "product_id":   f"P{i + 1:04d}",
            "product_name": fake.catch_phrase(),
            "category":     random.choice(CATEGORIES),
            "price":        round(random.uniform(99, 9999), 2),
        }
        for i in range(n)
    ]


def _generate_orders(customers: list, products: list, n: int) -> list:
    """Generate n fake order records linking customers and products.

    Args:
        customers: List of customer dicts.
        products:  List of product dicts.
        n:         Number of orders to generate.

    Returns:
        List of order dicts.
    """
    rows = []
    for _ in range(n):
        cust = random.choice(customers)
        prod = random.choice(products)
        qty  = random.randint(1, 5)
        rows.append({
            "order_id":   str(uuid.uuid4()),
            "customer_id": cust["customer_id"],
            "product_id":  prod["product_id"],
            "order_date":  _rand_date(_ONE_YEAR_AGO, _RANGE_1Y),
            "quantity":    qty,
            "amount":      round(prod["price"] * qty, 2),
            "status":      random.choice(STATUSES),
        })
    return rows


# ── CSV writer ────────────────────────────────────────────────────────────────


def _write_csv(rows: list, path: Path) -> None:
    """Write a list of dicts to a CSV file efficiently.

    Uses buffered writing (default 8 KB buffer) for speed.

    Args:
        rows: List of dicts — all must share the same keys.
        path: Output file path.

    Returns:
        None
    """
    with open(path, "w", newline="", buffering=8192) as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)   # writerows() is faster than looping writerow()
    print(f"  Saved {len(rows):,} rows → {path}")