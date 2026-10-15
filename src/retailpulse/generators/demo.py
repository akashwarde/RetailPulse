"""Repeatable demo sources with deliberately permissive raw sales staging."""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from psycopg2.extras import execute_values

from retailpulse.common.db import get_db_connection
from retailpulse.common.logging import get_logger

log = get_logger("generators")
PRODUCT_COUNT, CUSTOMER_COUNT, STORE_COUNT = 50, 1000, 5
SALES_COLUMNS = ["order_id", "order_item_id", "customer_id", "store_id", "product_id",
                 "quantity", "unit_price", "order_timestamp", "order_status", "payment_method", "source_date"]


def write_csv(path: Path, rows: list[dict], columns: list[str] | None = None) -> None:
    """Write a source CSV with a stable column order."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns or list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate(days: int = 90, defect_rate: float = 0.04, orders: int = 20000,
             end_date: str | None = None, seed: int = 42,
             output_dir: str | Path | None = None, load_db: bool = True) -> dict:
    """Create sources; replace only the generated date range in raw_sales.

    source_date preserves the intended partition for invalid future timestamps.
    Same seed and dates produce identical source files. Masters are upserted.
    """
    if days < 1 or orders < days or not 0 <= defect_rate <= 1:
        raise ValueError("days must be positive, orders >= days, defect_rate in [0, 1]")
    rng = random.Random(seed)
    end = date.fromisoformat(end_date) if end_date else date.today() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    root = Path(output_dir or os.getenv("DATA_DIR", "data/source"))
    inventory_dir = root / "inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)
    categories = ["Electronics", "Home", "Fashion", "Grocery", "Sports", "Books"]
    cities = ["Mumbai", "Pune", "Nashik", "Bengaluru", "Hyderabad"]
    products = [dict(product_id=i, product_name=f"Product {i}", category=categories[(i-1) % 6],
                     unit_price=round(rng.uniform(20, 5000), 2), active=True)
                for i in range(1, PRODUCT_COUNT + 1)]
    stores = [dict(store_id=i, store_name=f"{city} Store", city=city, state="India",
                   opened_date="2019-01-01") for i, city in enumerate(cities, 1)]
    customers = [dict(customer_id=i, first_name=f"Customer {i}", last_name="Demo",
                      email=f"customer{i}@example.com", city=rng.choice(cities), state="India",
                      signup_date="2024-01-01") for i in range(1, CUSTOMER_COUNT + 1)]
    clean_customers = [dict(row) for row in customers]
    for i in rng.sample(range(len(customers)), round(len(customers) * defect_rate)):
        defect = rng.randrange(3)
        if defect == 0 and i > 0:
            customers[i] = dict(customers[i-1])
        elif defect == 1:
            customers[i]["email"] = "bad-email"
        else:
            customers[i]["city"] = None
    sales = []
    for i in range(1, orders + 1):
        day = start + timedelta(days=(i - 1) % days)
        product = rng.choice(products)
        sales.append(dict(order_id=i, order_item_id=i, customer_id=rng.randint(1, CUSTOMER_COUNT),
                          store_id=rng.randint(1, STORE_COUNT), product_id=product["product_id"],
                          quantity=rng.randint(1, 5), unit_price=product["unit_price"],
                          order_timestamp=datetime.combine(day, datetime.min.time()).replace(
                              hour=rng.randrange(24), minute=rng.randrange(60)).isoformat(),
                          order_status="COMPLETED", payment_method=rng.choice(["CARD", "CASH", "UPI"]),
                          source_date=day.isoformat()))
    clean_sales = [dict(row) for row in sales]
    for i in rng.sample(range(len(sales)), round(len(sales) * defect_rate)):
        defect = rng.randrange(4)
        if defect == 0:
            sales[i]["customer_id"] = None
        elif defect == 1:
            sales[i]["quantity"] = -1
        elif defect == 2:
            sales[i]["order_timestamp"] = datetime.combine(end + timedelta(days=3650), datetime.min.time()).isoformat()
        elif i >= days:
            sales[i] = dict(sales[i-days])
        else:
            sales[i]["quantity"] = -1
    for offset in range(days):
        day = start + timedelta(days=offset)
        inventory = [dict(product_id=p["product_id"], store_id=s["store_id"], stock_qty=rng.randrange(100),
                          snapshot_date=day.isoformat()) for p in products for s in stores]
        for i in rng.sample(range(len(inventory)), round(len(inventory) * defect_rate)):
            if i % 3 == 0:
                inventory[i]["product_id"] = None
            elif i % 3 == 1:
                inventory[i]["stock_qty"] = -1
            else:
                inventory[i]["snapshot_date"] = day.strftime("%d/%m/%Y")
        write_csv(inventory_dir / f"inventory_{day.isoformat()}.csv", inventory)
    write_csv(root / "products.csv", products)
    write_csv(root / "stores.csv", stores)
    write_csv(root / "sales.csv", sales, SALES_COLUMNS)
    (root / "customers.json").write_text(json.dumps(customers, indent=2), encoding="utf-8")
    if load_db:
        with get_db_connection("source") as conn:
            try:
                with conn.cursor() as cur:
                    for table, rows, key in [("products", products, "product_id"), ("stores", stores, "store_id"),
                                              ("customers", clean_customers, "customer_id")]:
                        cols = list(rows[0])
                        update = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != key)
                        execute_values(cur, f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
                                       f"ON CONFLICT ({key}) DO UPDATE SET {update}",
                                       [tuple(r[c] for c in cols) for r in rows])
                    # Keep the normalized operational source populated too. Its
                    # constraints remain intact; deliberately bad copies land in raw_sales.
                    for table, cols, key in [
                        ("orders", ["order_id", "customer_id", "store_id", "order_timestamp", "order_status", "payment_method"], "order_id"),
                        ("order_items", ["order_item_id", "order_id", "product_id", "quantity", "unit_price"], "order_item_id"),
                    ]:
                        update = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != key)
                        execute_values(cur, f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s "
                                       f"ON CONFLICT ({key}) DO UPDATE SET {update}",
                                       [tuple(row[c] for c in cols) for row in clean_sales])
                    cur.execute("DELETE FROM raw_sales WHERE source_date BETWEEN %s AND %s", (start, end))
                    execute_values(cur, f"INSERT INTO raw_sales ({', '.join(SALES_COLUMNS)}) VALUES %s",
                                   [tuple(r[c] for c in SALES_COLUMNS) for r in sales])
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    counts = dict(orders=len(sales), customers=len(customers), products=len(products), stores=len(stores),
                  inventory_files=days, start_date=start.isoformat(), end_date=end.isoformat())
    log.info("Generated demo sources: %s", counts)
    return counts


def main() -> None:
    """Generate files and database staging; --files-only prepares local fixtures."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--orders", type=int, default=20000)
    parser.add_argument("--defect-rate", type=float, default=0.04)
    parser.add_argument("--end-date")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir")
    parser.add_argument("--files-only", action="store_true")
    args = vars(parser.parse_args())
    args["load_db"] = not args.pop("files_only")
    generate(**args)


if __name__ == "__main__":
    main()
