# Handoff: batch foundations and remaining orchestration work

This change completes the earlier-phase prerequisites. It does not add a consumer,
Airflow DAG, Metabase configuration, or final presentation. Those remain your work.
Changes were implemented on `main`, based on remote commit `0bab518`. The remote
default branch is `master`; integration uses a pull request from `main`.

## Functions the batch DAG can call

All dates are ISO strings (`YYYY-MM-DD`). Extractors return row counts, object
paths, and stable batch IDs; silver jobs return accepted/rejected counts and
raise `QualityGateError` above the 5% threshold. They log through the shared
logger; the DAG can attach its own failure callback and notifications.

```python
from retailpulse.ingestion.batch import (
    extract_masters, extract_sales, extract_inventory, extract_customers,
)
from retailpulse.transform import (
    run_silver_sales, run_silver_inventory, run_silver_customers,
)
from retailpulse.warehouse import load_dimensions, load_facts, refresh_marts
```

1. Generate demo data separately using `python -m retailpulse.generators`.
2. Run `extract_masters(ds)` before sales/inventory validation and dimension loads.
3. Each source group calls `extract_<source>(ds)` then `run_silver_<source>(ds)`.
   The silver function includes validation, quarantine, summary persistence, and
   the quality check; do not independently append the same DQ summary afterward.
4. After all three groups succeed, call `load_dimensions(ds)` then `load_facts(ds)`.
5. `refresh_marts(conn)` takes an open warehouse psycopg2 connection. A direct
   example is in `retailpulse.pipeline.run_batch`; it does not schedule anything.

Preserved entrypoints include the three transform module `main(run_date)`
functions and `warehouse.load_dimensions.main` / `warehouse.load_facts.main`.

## Streaming integration contract

The existing producer and topic provisioning are preserved. Producer IDs now use
the same 1–1,000 customer and 1–50 product ranges as the batch generator.

The consumer/DLQ code should store valid events under
`bronze/online_orders/dt=YYYY-MM-DD/` with an identity per micro-batch; avoid
overwriting the same `data.parquet` object with only the latest events. Commit
Kafka offsets only after durable output, and make retries reuse the batch identity.

The future online transform must retain `event_id`. The existing batch sales
transform is specifically for physical sales, so use an online transform for the
producer's event schema. The warehouse helper accepts producer `event_time` or
normalized `order_timestamp` and supports nullable store IDs for online sales:

```python
from retailpulse.warehouse import load_fact_sales

result = load_fact_sales(
    conn, data=clean_events, run_date=ds, channel="online", batch_id=stable_batch_id,
)
```

Each event requires `event_id`, `order_id`, `customer_id`, `product_id`, positive
integral `quantity`, positive finite `unit_price`, and its event timestamp.
`order_item_id` is optional for online events. Replay ignores an existing event ID;
online micro-batches never delete earlier online or physical facts. `loaded`
reports distinct accepted input rows, including already-stored replayed events.
Warehouse lookup failures are persisted to PostgreSQL quarantine; consumer
validation/DLQ routing and its own lake quarantine remain the consumer's job.

For the Python consumer, `retailpulse.common.records.persist_rejections` can
persist a list of dictionaries with `rule_failed`, `reason`, and `raw_record`.
Pass `replace=False` for incremental events; the caller owns commit/rollback.

Use `write_quarantine` with a unique stable micro-batch ID when calling the shared
Spark quarantine writer. It replaces that batch's rows, including an empty result.
`write_quality_summary` upserts the same source/batch ID rather than appending it.

## Shared files to reconcile once

These shared files changed to fix the prerequisites; merge these versions before
adding the remaining features rather than replacing the files wholesale:

- `.env.example`, `compose.yaml`, `docker/airflow/Dockerfile`, and PostgreSQL/Airflow
  initialization scripts: application settings, SQL assets, Java portability,
  initialization dependencies, and the missing Airflow session-secret setting.
- `requirements-dev.txt`, Makefile, README and CHANGELOG: test commands and usage.
- `common/config.py`, validation, batch transforms and warehouse modules: corrected
  behavior behind the existing public entrypoints.
- `ingestion/stream/order_generator.py`: only the shared master-ID ranges changed.
- `tests/conftest.py`: shared Spark fixture and opt-in isolated PostgreSQL fixture.
  Extend these fixtures when adding tests. Existing validation, dimension/fact-load,
  and mart tests were updated to assert the corrected behavior; new regression
  tests live in separate files so the remaining smoke test can be added independently.

No edits were made to `compose.streaming.yml`, `order_producer.py`, or base DQ/DW
DDL files. New migrations are separate SQL files, applied by
`python -m retailpulse.common.initialize`; run it on existing volumes too.

Reserved for your additions: `dags/`, consumer/DLQ modules, an online silver
transform, `compose.bi.yml`, dashboard exports, alert integration and the final
Airflow-driven smoke script. Git cannot guarantee zero conflicts with unshared
local edits; this file identifies the integration points in advance.

The supplied parent-folder `env.example` is preserved. The repository template
contains placeholders, the same service setting names, and the additional
`AIRFLOW_WEBSERVER_SECRET_KEY`. Keep host endpoints (`localhost`) in a host `.env`;
Compose supplies container endpoints (`postgres`, `minio`, `redpanda`) explicitly.
