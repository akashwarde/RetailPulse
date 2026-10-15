"""
### RetailPulse daily batch pipeline

```
start -> [ sales | inventory | customers : extract -> to_silver ] + extract_masters
      -> load_dimensions -> load_facts -> refresh_marts -> write_dq_summary -> end
```

* **extract** reads the source system for the run date and writes the *bronze* partition.
* **to_silver** validates the rows, quarantines bad ones with the reason, writes the quality
  summary and **enforces the quality gate** (the task - and so the DAG - fails when the
  rejected share exceeds `QUALITY_GATE_THRESHOLD`, default 5%).
* the warehouse loads are idempotent (delete-and-reload by date / upsert), so any task or the
  whole run can be re-run safely.
* `write_dq_summary` always runs (even after a failure) and records the quality outcome.

The DAG file only wires tasks together; all logic lives in `src/retailpulse/`.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from retailpulse import pipeline
from retailpulse.dag_defaults import DEFAULT_ARGS, SPARK_POOL

SOURCES = ["sales", "inventory", "customers"]


@dag(
    dag_id="retail_batch_pipeline",
    description="Daily batch: sources -> bronze -> validated silver -> warehouse -> marts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["retailpulse", "batch"],
    doc_md=__doc__,
)
def retail_batch_pipeline():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def extract(source: str, ds: str | None = None):
        return pipeline.extract_stage(source, ds)

    @task(pool=SPARK_POOL)
    def to_silver(source: str, ds: str | None = None):
        return pipeline.silver_stage(source, ds)

    @task
    def load_dimensions(ds: str | None = None):
        return pipeline.load_dimensions_stage(ds)

    @task
    def load_facts(ds: str | None = None):
        return pipeline.load_facts_stage(ds)

    @task
    def refresh_marts():
        return pipeline.refresh_marts_stage()

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def write_dq_summary(ds: str | None = None, run_id: str | None = None):
        from retailpulse.monitoring import write_dq_summary as _write

        return _write(ds, dag_id="retail_batch_pipeline", run_id=run_id)

    # product/store masters are needed by the sales and inventory referential checks
    extract_masters = extract.override(task_id="extract_masters")("masters")
    first_tasks, silver_tasks = [extract_masters], [extract_masters]
    for source in SOURCES:
        with TaskGroup(group_id=source):
            e = extract.override(task_id="extract")(source)
            s = to_silver.override(task_id="to_silver")(source)
            e >> s
            if source in ("sales", "inventory"):
                extract_masters >> s
        first_tasks.append(e)
        silver_tasks.append(s)

    dims = load_dimensions()
    facts = load_facts()
    marts = refresh_marts()
    dq = write_dq_summary()

    start >> first_tasks
    silver_tasks >> dims >> facts >> marts >> dq
    [marts, dq] >> end


retail_batch_pipeline()
