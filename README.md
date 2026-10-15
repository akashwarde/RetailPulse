# RetailPulse - end-to-end data engineering platform

Batch and streaming retail pipeline: **sources -> bronze/silver lake (MinIO, Parquet) -> PySpark validation -> PostgreSQL star schema -> Metabase**, orchestrated by **Airflow**, with a streaming path on **Redpanda (Kafka)**.

```
raw_sales (Postgres) -+                              +- quarantine (MinIO + dq.quarantine)
Inventory CSVs -------+-> bronze -> PySpark silver -> quality gate -> dw star schema -> marts -> Metabase
customers.json -------+                                   ^
Order producer -> Redpanda -> consumer -> bronze/silver --+   (invalid events -> orders_dlq)
```

## Quick start (Docker Compose v2; macOS, Linux, or Windows via WSL2)

```bash
make env          # creates .env with random passwords
make up           # Postgres, MinIO, Airflow, Redpanda, Metabase  (make up-lite = no streaming/BI)
make wait         # until Airflow is healthy
make demo         # init DB, 30 days of data with defects, batch DAG, streaming DAG, Metabase dashboard
make smoke        # 18 end-to-end assertions
make test         # unit tests inside the Airflow image
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / `AIRFLOW_ADMIN_PASSWORD` |
| MinIO console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Redpanda console | http://localhost:8085 | - |
| Metabase | http://localhost:3000 | `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` |
| PostgreSQL | localhost:5432 | `POSTGRES_USER` / `POSTGRES_PASSWORD` |

Useful targets: `make batch RUN_DATE=2026-10-14` (DAG run), `make batch-direct` (same stages without the scheduler),
`make backfill DAYS=7`, `make init` (re-apply DDL on an existing volume), `make help`.

## Failure demos
- `make gate-demo` - regenerates the day with 20% defects; the quality gate fails the DAG, the failure is written to `dq.pipeline_events` and `logs/alerts.log` (and to a webhook if `ALERT_WEBHOOK_URL` is set).
- `make recover` - regenerates clean data and re-runs; reruns are idempotent.

## Pipeline summary
- **Batch DAG `retail_batch_pipeline`** (daily): `extract_masters` + per-source `extract -> to_silver` (Spark pool of 2) -> `load_dimensions` -> `load_facts` -> `refresh_marts`; `write_dq_summary` always runs (`all_done`).
- **Stream DAG `retail_stream_microbatch`** (every 5 min, one active run): consume -> short-circuit if nothing new -> silver -> `load_fact_sales` (channel `online`, replay-safe on `event_id`) -> stream quality gate.
- **Quality gates**: batch rejects above `QUALITY_GATE_THRESHOLD` (5%) fail the task; stream above `STREAM_QUALITY_THRESHOLD` (15%, min 50 messages).
- **Idempotency**: in-store facts replace whole date snapshots; online facts are `ON CONFLICT (event_id)`; DQ rows are upserted per stable batch id.
- **Delivery**: consumer commits Kafka offsets only after bronze, quarantine, DLQ and summary writes succeed.

## Sources and retry behaviour
The generator (`python -m retailpulse.generators`) creates 50 products, 5 stores, 1,000 customers, sales rows with injected defects, and daily inventory CSVs (`--days`, `--orders`, `--defect-rate`, `--end-date`, `--seed`, `--files-only`). `raw_sales` is a deliberately permissive landing table. Details in [data contracts](docs/data_contracts.md); lake paths in [lake layout](docs/lake_layout.md).

## Documentation
[architecture](docs/architecture.md) - [data contracts](docs/data_contracts.md) - [runbook](docs/runbook.md) - [implementation notes](docs/implementation_notes.md) - [report/presentation outline](docs/report_and_presentation_outline.md)

## Tests
`make test` (Docker) or `make test-local` (needs Java 17). Optional live PostgreSQL tests need `RETAILPULSE_TEST_DSN` pointing at a disposable server; each test creates and drops its own database.

## Verification status
Verified without Docker, against PostgreSQL 16 and an S3 emulator: full batch end-to-end through `airflow dags test`, quality-gate failure with alerts and recovery, reruns without duplicates, streaming silver + online fact load (including replay), all 13 dashboard SQL cards, 18-check smoke test (direct mode), unit tests.
**Not yet run live:** the full Docker Compose stack, real Redpanda, Metabase API provisioning, and the Airflow-triggered smoke mode. Run `make up && make demo && make smoke` first.
