# Changelog

## Unreleased

### Orchestration, streaming, alerts and BI
- Stream consumer with validation, quarantine, DLQ (`orders_dlq`) and commit-after-write offsets; online silver transform
- Airflow DAGs `retail_batch_pipeline`, `retail_stream_microbatch`, `generate_demo_data`; `spark` pool
- Configurable batch/stream quality gates; failure callbacks, `dq.pipeline_events`, log and webhook alerts
- Metabase dashboard (13 cards) provisioned from `dashboards/cards.json`
- Verified end to end on Docker Desktop (Apple Silicon): `make smoke` passes all 23 checks
- App-level `ensure_databases()` creates `warehouse_db`/`airflow_db` when init scripts cannot run (Docker Desktop bind mounts)
- Configurable host Postgres port (`POSTGRES_HOST_PORT`); unique logical time per manual trigger; smoke run detection via `list-runs`
- Smoke test (`make smoke`), Makefile targets for demo, gate-demo, recover and backfill; docs and runbook

### Foundations

- Add deterministic multi-source demo data, PostgreSQL raw sales staging, three
  date-scoped batch extractors, and validated product/store master publication.
- Repair DQ connection calls, stable batch identities, retry-safe summaries and
  quarantine replacement, per-rule reporting, and validation/deduplication.
- Preserve online micro-batches with event-id replay protection; persist failed
  warehouse lookups; replace corrected/empty batch snapshots atomically.
- Wire application environment, SQL initialization, service dependencies, portable
  Java paths, and direct batch/test commands into the Compose setup.
- Add component and isolated PostgreSQL regression coverage and usage documentation.
