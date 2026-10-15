# RetailPulse

Batch and streaming retail analytics using PostgreSQL, MinIO, PySpark, Airflow,
and Redpanda. The generator, batch extractors, silver transforms, warehouse loads,
and SQL marts are implemented. Airflow DAGs, the stream consumer/DLQ workflow,
and Metabase are still pending.

## Run the batch components

Use Docker Compose v2 with `include` support. On Windows use WSL2 and keep the
checkout in the Linux filesystem; the commands below also work on macOS.

```bash
cp .env.example .env
# Set the passwords and AIRFLOW_WEBSERVER_SECRET_KEY in .env.
make up
docker compose --profile core ps -a
make generate
# The generator ends yesterday by default. Use a generated date here.
make batch DATE=2026-10-14
make test
```

Without Make, use `docker compose --profile core up -d --build`, then
`docker compose --profile core exec airflow-scheduler python -m retailpulse.generators`.
Run a date with `docker compose --profile core exec airflow-scheduler python -m retailpulse.pipeline YYYY-MM-DD`.
The batch command executes Python components directly; scheduling will be added
by the Airflow DAG work.

Airflow initialization applies source/warehouse SQL and database migrations before
the scheduler/webserver start. The MinIO initializer creates the lake bucket.
Running initialization again is supported:

```bash
make init
```

For an existing database volume created before configurable database names were
added, keep the original database names unless you create the replacement
databases first. Initialization does not drop databases or recreate volumes.
The retry-key migration consolidates duplicate DQ records for the same logical
batch, retaining the most recently inserted duplicate.

## Services

| Service | Host address | Login |
|---|---|---|
| Airflow | http://localhost:8080 | admin / AIRFLOW_ADMIN_PASSWORD |
| PostgreSQL | localhost:5432 | POSTGRES_USER / POSTGRES_PASSWORD |
| MinIO API | http://localhost:9000 | MINIO_ROOT_USER / MINIO_ROOT_PASSWORD |
| MinIO console | http://localhost:9001 | same MinIO credentials |
| Redpanda Kafka | localhost:19092 | local development broker |
| Redpanda console | http://localhost:8085 | local development console |

Enable streaming infrastructure with
`docker compose --profile core --profile streaming up -d --build`.
Only the producer and broker/topic provisioning are implemented at this point.

## Sources and retry behavior

The generator creates 50 products, 5 stores, 1,000 customer JSON records,
20,000 sales rows over 90 days, and daily inventory CSVs. Use `--days`, `--orders`,
`--defect-rate`, `--end-date`, `--seed`, or `--files-only` to customize it.
The normalized source `customers`, `orders`, and `order_items` tables contain clean
counterparts. `raw_sales` deliberately permits malformed copies that operational
constraints would reject. It is the batch sales extraction source; see
[data contracts](docs/data_contracts.md).

Extractors overwrite one bronze date partition. Inventory imports use content
fingerprints and skip unchanged successful files. Products/stores are validated
and published to bronze/silver before dimension loads.

Sales/inventory/customer silver jobs replace the same date partition and use stable
logical batch IDs. DQ summaries are upserted and quarantine batches replaced.
The 5% quality gate stops publication of a bad silver batch; a failure leaves the
last successful silver partition available for inspection. Warehouse stages must
run only after all current silver stages succeed.

In-store sales and inventory loads replace complete date snapshots, including an
explicitly empty corrected snapshot. Online sales are incremental: each event
requires an `event_id`, and replay uses `ON CONFLICT DO NOTHING`. Online loads do
not delete date partitions. Missing dimension lookups are persisted in PostgreSQL
quarantine in the fact transaction. Load-result `loaded` counts accepted distinct
input rows, including events already present during a replay.

## Tests

`make test` executes tests inside the Airflow image. Optional live database tests
require `RETAILPULSE_TEST_DSN` pointing to a disposable test server with database
creation permissions. Each test creates and removes its own database; production
connections are not used by default. Example inside the Compose network:

```bash
docker compose --profile core exec \
  -e RETAILPULSE_TEST_DSN='host=postgres user=retail password=YOUR_TEST_PASSWORD dbname=postgres' \
  -e RETAILPULSE_TEST_MINIO=1 \
  airflow-scheduler python -m pytest tests -q
```

See [implementation notes](docs/implementation_notes.md) for the completed scope,
validation evidence, and the remaining work owned by the DAG/streaming/BI phases.
