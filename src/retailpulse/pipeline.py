"""Callable batch stages for manual execution and future Airflow DAG tasks."""
from __future__ import annotations

import argparse

from retailpulse.ingestion.batch import extract_customers, extract_inventory, extract_masters, extract_sales


def run_batch(run_date: str) -> dict:
    """Run a logical date through the existing batch components."""
    from retailpulse.transform import run_silver_customers, run_silver_inventory, run_silver_sales
    from retailpulse.warehouse import load_dimensions, load_facts
    from retailpulse.common.db import get_db_connection
    from retailpulse.warehouse.marts import refresh_marts
    result = {"masters": extract_masters(run_date)}
    for name, extract, transform in [("customers", extract_customers, run_silver_customers),
                                      ("sales", extract_sales, run_silver_sales),
                                      ("inventory", extract_inventory, run_silver_inventory)]:
        result[name] = {"bronze": extract(run_date), "silver": transform(run_date)}
    result["dimensions"] = load_dimensions(run_date)
    result["facts"] = load_facts(run_date)
    with get_db_connection("warehouse") as conn:
        result["marts"] = refresh_marts(conn)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_date")
    run_batch(parser.parse_args().run_date)
