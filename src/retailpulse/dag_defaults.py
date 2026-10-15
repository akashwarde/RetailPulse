"""Settings shared by all RetailPulse DAGs (kept free of Airflow imports)."""

from __future__ import annotations

from datetime import timedelta

from retailpulse.monitoring import on_failure_callback

DEFAULT_ARGS = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": on_failure_callback,
}

SPARK_POOL = "spark"   # created by airflow-init; limits concurrent local Spark JVMs on small laptops
