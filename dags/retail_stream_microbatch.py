"""
### RetailPulse streaming micro-batch (every 5 minutes)

```
consume -> has_new_data? -> to_silver -> load_fact_sales -> stream_quality_gate -> log_counts
```

* **consume** reads up to N messages / T seconds from `orders_stream`; valid events go to bronze,
  invalid ones to the `orders_dlq` topic and `dq.quarantine`. Kafka offsets are committed only
  after those writes succeed.
* **has_new_data?** short-circuits the rest when nothing valid arrived.
* **load_fact_sales** appends to `dw.fact_sales` (channel `online`), idempotent by `event_id`.
* **stream_quality_gate** fails the run when too many events were invalid. It runs *after* the
  load, so good events are never held back by a noisy batch.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.decorators import dag, task

from retailpulse import pipeline
from retailpulse.dag_defaults import DEFAULT_ARGS

MAX_MESSAGES = int(os.getenv("STREAM_MAX_MESSAGES", "500"))
MAX_SECONDS = float(os.getenv("STREAM_MAX_SECONDS", "45"))


@dag(
    dag_id="retail_stream_microbatch",
    description="Every 5 min: Redpanda orders_stream -> validate -> bronze/DLQ -> silver -> fact_sales",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,                       # micro-batches must never overlap
    default_args={**DEFAULT_ARGS, "retries": 1},
    tags=["retailpulse", "streaming"],
    doc_md=__doc__,
)
def retail_stream_microbatch():
    @task
    def consume():
        return pipeline.stream_consume_stage(MAX_MESSAGES, MAX_SECONDS)

    @task.short_circuit(task_id="has_new_data")
    def has_new_data(batch: dict) -> bool:
        return bool(batch.get("bronze_paths"))

    @task
    def to_silver(batch: dict):
        return pipeline.stream_silver_stage(batch["bronze_paths"])

    @task
    def load_fact_sales(silver: dict):
        return pipeline.stream_load_stage(silver["silver_paths"])

    @task
    def quality_gate(batch: dict, loaded: dict):
        from retailpulse.monitoring import stream_quality_gate

        return stream_quality_gate(batch)

    @task(trigger_rule="all_done")
    def log_counts(batch: dict, ti=None):
        from retailpulse.monitoring import record_event

        record_event(
            "retail_stream_microbatch", "INFO",
            f"consumed={batch.get('consumed')} valid={batch.get('valid')} invalid={batch.get('invalid')} "
            f"late={batch.get('late')} dlq={batch.get('dlq_sent')} stop={batch.get('stop_reason')}",
            task_id="log_counts", run_id=getattr(ti, "run_id", None))

    batch = consume()
    gate = has_new_data(batch)
    silver = to_silver(batch)
    loaded = load_fact_sales(silver)
    checked = quality_gate(batch, loaded)
    gate >> silver
    batch >> log_counts(batch)
    loaded >> checked


retail_stream_microbatch()
