"""
### Generate demo data (manual trigger)

Rebuilds the simulated source systems: the Postgres sales DB, the daily inventory CSVs and the
customer JSON feed -- with deliberately injected defects. Re-running replaces the previous data,
so the demo can be repeated. Parameters:

* `days` - days of history (default 30)
* `defect_rate` - fraction of injected defects. The default (0.04) passes the 5% quality gate;
  try `0.15` to watch the gate stop the pipeline.
"""

from __future__ import annotations

from datetime import date, datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from retailpulse.dag_defaults import DEFAULT_ARGS


@dag(
    dag_id="generate_demo_data",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={**DEFAULT_ARGS, "retries": 0},
    params={
        "days": Param(30, type="integer", minimum=1, maximum=365),
        "defect_rate": Param(0.04, type="number", minimum=0, maximum=0.5),
    },
    tags=["retailpulse", "demo"],
    doc_md=__doc__,
)
def generate_demo_data():
    @task
    def bootstrap_and_generate(params: dict | None = None):
        import os

        from retailpulse.common.initialize import main as apply_ddl
        from retailpulse.generators.demo import generate

        apply_ddl()
        days = int(params["days"])
        return generate(days=days, orders=days * 250, defect_rate=float(params["defect_rate"]),
                        output_dir=os.getenv("DATA_DIR", "data/source"))

    bootstrap_and_generate()


generate_demo_data()
