# Architecture

| Layer | Tech | Code |
|---|---|---|
| Sources | Postgres OLTP (no constraints, so defects can arrive), inventory CSV, customers JSON, Kafka orders | `generators/demo.py`, `ingestion/stream/order_producer.py` |
| Bronze | Parquet on MinIO, lineage columns | `ingestion/batch/extractors.py`, `ingestion/stream/order_consumer.py` |
| Silver | PySpark (batch), pandas (stream); rule engine | `transform/`, `validation/` |
| Quality | not_null / range / regex / unique rules, quarantine, gate | `validation/`, `monitoring.py` |
| Warehouse | Postgres star schema `dw`, 5 mart views | `sql/ddl`, `warehouse/` |
| Orchestration | Airflow 2.10.5 TaskFlow DAGs | `dags/`, `pipeline.py` |
| BI | Metabase, provisioned via REST | `dashboards/` |

## Batch DAG
start → extract_masters + per-source (extract → to_silver (sales and inventory wait for the masters), `spark` pool of 2) → load_dimensions → load_facts → refresh_marts → end; `write_dq_summary` runs with `all_done` so a failure is always reported.

## Stream DAG (every 5 min, one active run)
consume → has_new_data (short-circuit) → to_silver → load_fact_sales → quality_gate. The gate runs after the load so valid events are never held back. Offsets are committed only after bronze, quarantine, DLQ and summary writes succeed → at-least-once delivery + idempotent sinks = effectively-once.

## Idempotency
Batch facts: delete-and-reload per date. Stream facts: unique `event_id` + `ON CONFLICT DO NOTHING`. DQ tables: stable batch id with upserted summaries and replaced quarantine batches (unique indexes from `05_retry_keys.sql`).
