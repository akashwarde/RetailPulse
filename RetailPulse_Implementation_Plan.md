# RetailPulse: Implementation Plan (3 members)

**Project:** End-to-end batch + streaming data pipeline for retail business intelligence
**Team:** 3 members (Victus 15 / MacBook / MSI) | **Orchestration:** Apache Airflow | **Everything runs in Docker**

> **One-line story:** *Retail data arrives messy from four different systems. RetailPulse ingests it (batch and streaming), validates and cleans it, stores it in a lake and a warehouse, and serves KPIs on a dashboard, all scheduled and monitored by Airflow.*

---

## 1. Architecture at a glance

```
SOURCES                 INGESTION             STORAGE (medallion)            SERVING
                                              
Sales DB (Postgres) --\                       Bronze (raw, MinIO)
Inventory CSV files ---+-> Batch ingestion -> Silver (clean, MinIO) ----\
Customer JSON/API  ---/    (Python/Spark)     Quarantine (bad rows)      \
                                                                          +-> Warehouse (Postgres, star schema) -> Metabase dashboard
Online store events -> Redpanda topic -> Stream consumer -> Bronze/Silver /       (dims + facts + marts)
 (simulated orders)      (+ DLQ)        (micro-batch)

                 ALL STEPS SCHEDULED, RETRIED AND MONITORED BY AIRFLOW
```

| Layer | Tool | Why (simple to explain) |
|---|---|---|
| Sources | Postgres (sales), CSV (inventory), JSON (customers), Redpanda (online orders) | The 4 systems in the problem statement |
| Data generator | Python + Faker | We control the data and **inject defects on purpose** (nulls, duplicates, bad formats) |
| Batch ingestion | Python + PySpark (local mode) | Reuses what we learned in the Spark case study |
| Streaming | Redpanda + Python consumer (micro-batch) + DLQ | Reuses the Experiment 6 lab |
| Lake | MinIO (S3-compatible), Parquet files | Cheap raw storage, replayable |
| Validation | Custom rule engine (or Pandera) + quarantine table | Missing/inconsistent records handled visibly |
| Warehouse | PostgreSQL, star schema (separate DB from the source DB) | Analytical queries |
| Orchestration | Airflow | Automation, retries, dependencies, scheduling |
| Dashboard | Metabase | KPIs without writing a web app |

**Why Postgres as the warehouse:** free, runs on all 3 laptops, and everyone already knows SQL. In the report, mention that cloud warehouses (BigQuery, Snowflake, Redshift) play the same role.

---

## 2. Hardware and environment rules (3 different laptops)

The GPU (RTX 3050) is **not needed**. Nothing in this project uses it.

| Machine | Likely OS | Setup |
|---|---|---|
| **Victus 15** | Windows | WSL2 + Docker Desktop (WSL backend). Clone the repo **inside the WSL filesystem** (`~/projects`), not in `C:\`. |
| **MSI** | Windows | Same as Victus. |
| **MacBook** | macOS | Docker Desktop or OrbStack. If it is Apple Silicon, use only images that have an `arm64` build. |

**Rules to avoid "works on my machine":**
1. **Docker Compose is the only way to run the project.** No local installs of Airflow, Spark, Postgres, etc.
2. **Pin every image and package version.** Never use `latest`.
3. **Only use multi-arch images** (amd64 and arm64). Check Docker Hub before adding any image.
4. `.gitattributes` with `* text=auto eol=lf`, so Windows does not break shell scripts with CRLF line endings.
5. **Compose profiles** to save RAM: `core` (Postgres, MinIO, Airflow), `streaming` (Redpanda), `bi` (Metabase). A machine with 8 GB RAM runs only what it needs. On Windows, limit WSL memory in `%UserProfile%\.wslconfig` (`memory=6GB`).
6. **Week 1 gate:** `docker compose --profile core up` must work on **all 3 machines** before anyone builds features. Check each laptop's RAM now. If one has 8 GB, give that person the lighter modules.
7. Secrets and paths go in `.env` (never committed). Provide `.env.example`.

**Suggested role fit (adjust if you prefer):** MacBook owner takes Airflow and Docker (Unix-native, fewest path problems). Victus owner takes ingestion and streaming. MSI owner takes storage, validation and warehouse.

---

## 3. Team split (by pipeline stage, so everyone can explain their part)

| Member | Pipeline stages owned | Main deliverables |
|---|---|---|
| **A: Ingestion & Streaming** | Sources, generator, batch ingestion, Redpanda producer/consumer, DLQ | Synthetic data generator, 3 batch extractors, streaming producer and consumer |
| **B: Storage, Validation & Transformation** | Lake layout (bronze/silver/quarantine), validation rules, PySpark cleaning | Data contracts, validation engine, bronze→silver jobs, quality report |
| **C: Warehouse, Orchestration & Dashboard** | Star schema, warehouse loads, Airflow DAGs, Docker/CI, Metabase | Compose stack, DDL and load SQL, 3 DAGs, dashboard with KPIs |

**Cross-rules:** each member writes the Airflow *tasks* for their own stage. Member C owns the DAG files and the Airflow platform. Each member must be able to explain the code of **one other** member's stage in the viva (swap-explain rehearsal in the last week).

---

## 4. Git workflow rules (non-negotiable)

1. **`main` is always runnable.** Nobody pushes directly to `main`. Enable branch protection (require a PR and 1 approval).
2. **One branch per feature**, branched from the latest `main`:
   `feature/<area>/<short-name>` (e.g., `feature/ingestion/sales-extract`), plus `fix/<short-name>` and `docs/<short-name>`.
3. **Short-lived branches** (max 2 to 3 days). If a feature is bigger, split it.
4. **Commit messages:** `type(area): message`, e.g., `feat(ingestion): add inventory CSV loader`. Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`.
5. **Definition of Done (a feature may be merged only if):**
   - [ ] Code runs inside Docker on your machine
   - [ ] Has at least a basic test or a documented manual test
   - [ ] README or `docs/` updated (how to run it)
   - [ ] `CHANGELOG.md` entry added
   - [ ] PR reviewed and approved by **a different member**
   - [ ] After merge, `main` still starts with `docker compose up` (smoke test)
6. **Repo is updated after each completed feature:** merge the PR (squash), delete the branch, move the card to Done on the project board, and everyone runs `git pull` on `main` before starting the next feature.
7. **Sync daily:** `git pull --rebase origin main` into your branch every morning.
8. **Tag each phase:** `v0.1-foundation`, `v0.2-ingestion`, and so on. These are your restore points before the demo.
9. **Never commit:** `.env`, data files, `*.parquet`, logs, `__pycache__`. Commit `.env.example` only.
10. **Avoid merge conflicts:** separate compose files per service group, separate requirements per module, and **one DAG file per pipeline, owned by one person**. If you need to change someone else's file, tell them first.
11. **AI-generated code rule:** you may use AI, but the author must read it, run it and be able to explain every line. Write what it does in the PR description.
12. **Track work** in GitHub Issues or a Projects board (Backlog / In progress / Review / Done), one issue per feature in Section 7.

---

## 5. Repository structure

```
retailpulse/
├── README.md  CHANGELOG.md  Makefile
├── .env.example  .gitignore  .gitattributes
├── docker-compose.yml            # core: postgres, minio, airflow
├── compose.streaming.yml         # redpanda (+ console)
├── compose.bi.yml                # metabase
├── docker/airflow/Dockerfile     # airflow + Java + pyspark + project deps
├── dags/
│   ├── retail_batch_pipeline.py
│   ├── retail_stream_microbatch.py
│   └── generate_demo_data.py
├── src/retailpulse/
│   ├── common/          # config, logging, db and minio helpers
│   ├── generators/      # fake data generator (with injected defects)
│   ├── ingestion/       # batch/ and stream/
│   ├── validation/      # rules, quarantine writer, quality report
│   ├── transform/       # bronze->silver PySpark jobs
│   └── warehouse/       # python helpers for loads
├── sql/                 # ddl/  load/  marts/
├── tests/
├── dashboards/          # exported Metabase questions + screenshots
└── docs/                # architecture.md, data_contracts.md, runbook.md
```

---

## 6. Data design

### 6.1 Source data and the defects we inject on purpose

| Source | Format | Content | Injected defects (for validation to catch) |
|---|---|---|---|
| Sales DB | Postgres tables `orders`, `order_items`, `stores` | In-store sales | Null `customer_id`, negative quantity, future dates, duplicate order IDs |
| Inventory files | Daily CSV drops | `product_id, store_id, stock_qty, snapshot_date` | Mixed date formats, missing product IDs, stock below 0, header changes |
| Customer platform | JSON file / small API | Customer profile | Duplicate customers, bad emails, missing city |
| Online store | Redpanda topic `orders_stream` | Order events | Malformed JSON, missing fields, late events |

Keep a small master data set: about 50 products, 5 stores, 1,000 customers, 20,000+ orders.

### 6.2 Validation rules (examples; write the full list in `docs/data_contracts.md`)

| Rule type | Example | If it fails |
|---|---|---|
| Not null | `order_id`, `product_id`, `customer_id` | Row goes to quarantine |
| Range | `quantity > 0`, `unit_price > 0`, `stock_qty >= 0` | Quarantine |
| Format | valid email, ISO date | Try to fix, else quarantine |
| Uniqueness | one row per `order_id + line_no` | Keep the first, log duplicates |
| Referential | `product_id` exists in the product master | Quarantine |
| Threshold | more than 5% rows rejected in a batch | **The DAG fails and alerts** (quality gate) |

Every rejected row is stored with `source`, `rule_failed`, `reason`, `batch_id`, `ingested_at`. The dashboard shows a **Data Quality scorecard** from this.

### 6.3 Warehouse star schema (Postgres, schema `dw`)

| Table | Type | Key columns |
|---|---|---|
| `dim_date` | dimension | date_key, day, month, quarter, year, is_weekend |
| `dim_product` | dimension | product_key, product_id, name, category, price |
| `dim_customer` | dimension | customer_key, customer_id, city, segment |
| `dim_store` | dimension | store_key, store_id, city, type (physical/online) |
| `fact_sales` | fact | date_key, product_key, customer_key, store_key, quantity, revenue, channel |
| `fact_inventory` | fact | date_key, product_key, store_key, stock_qty |
| Marts (views) | aggregates | `mart_daily_revenue`, `mart_top_products`, `mart_low_stock`, `mart_dq_summary` |

Loads are **idempotent**: re-running the same day must not create duplicates (`INSERT ... ON CONFLICT` or delete-and-reload by date).

### 6.4 Dashboard KPIs (Metabase)
Revenue trend by day, top 10 products, sales by category, store performance, online vs in-store share, low-stock alerts, and a data-quality scorecard (rows accepted vs rejected per source).

---

## 7. Feature backlog (one feature = one branch = one PR)

Phases are ordered. Do not start a phase until the previous phase's tag exists.

### Phase 0: Foundation (Week 1)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 0.1 | `feature/infra/repo-scaffold` | C | Repo structure, `.gitignore`, `.gitattributes`, `.env.example`, README skeleton, PR template, branch protection on |
| 0.2 | `feature/infra/compose-core` | C | Postgres (source + warehouse DBs), MinIO and Airflow start with `docker compose --profile core up` **on all 3 machines** |
| 0.3 | `feature/common/config-logging` | B | Shared config loader, logger, Postgres and MinIO helper functions |
| 0.4 | `docs/architecture-and-contracts` | A | Architecture diagram and first version of `data_contracts.md` |

Tag: `v0.1-foundation`

### Phase 1: Sources and ingestion (Week 2)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 1.1 | `feature/ingestion/data-generator` | A | One command creates sales tables, inventory CSVs, customer JSON, all with injected defects |
| 1.2 | `feature/ingestion/sales-extract` | A | Postgres sales → bronze Parquet in MinIO, partitioned by date |
| 1.3 | `feature/ingestion/inventory-csv` | A | CSV folder → bronze, handles bad headers and files already processed |
| 1.4 | `feature/ingestion/customer-json` | A | JSON/API → bronze |
| 1.5 | `feature/storage/lake-layout` | B | Bucket and folder convention (`bronze/<source>/dt=YYYY-MM-DD/`), documented |

Tag: `v0.2-ingestion`

### Phase 2: Validation and transformation (Week 2 to 3)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 2.1 | `feature/validation/rule-engine` | B | Rules defined per source in a config file, a function returns (good rows, bad rows) |
| 2.2 | `feature/validation/quarantine-and-report` | B | Bad rows saved with reason, quality summary table written |
| 2.3 | `feature/transform/silver-sales` | B | PySpark: dedupe, type casting, standardize, write silver |
| 2.4 | `feature/transform/silver-inventory-customers` | B | Same for inventory and customers |
| 2.5 | `feature/warehouse/ddl-star-schema` | C | All dim/fact tables created by SQL scripts |

Tag: `v0.3-silver`

### Phase 3: Warehouse (Week 3)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 3.1 | `feature/warehouse/load-dimensions` | C | Dims loaded from silver, idempotent |
| 3.2 | `feature/warehouse/load-facts` | C | `fact_sales` and `fact_inventory` loaded with key lookups |
| 3.3 | `feature/warehouse/marts` | C | The 4 mart views return correct results (check against a manual SQL count) |

Tag: `v0.4-warehouse`

### Phase 4: Streaming and Airflow (Week 4)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 4.1 | `feature/streaming/compose-redpanda` | C | Redpanda profile works on all 3 machines |
| 4.2 | `feature/streaming/order-producer` | A | Simulated online orders published, some malformed on purpose |
| 4.3 | `feature/streaming/consumer-dlq` | A | Consumer validates, writes good events to bronze, bad ones to a DLQ topic and quarantine |
| 4.4 | `feature/airflow/batch-dag` | C (tasks from A and B) | `retail_batch_pipeline` runs end to end daily |
| 4.5 | `feature/airflow/stream-dag` | C (A's consumer) | `retail_stream_microbatch` runs every 5 minutes |
| 4.6 | `feature/airflow/quality-gate-alerts` | B | DAG fails when the reject threshold is exceeded; failure callback logs and notifies |

Tag: `v0.5-orchestrated`

### Phase 5: Dashboard, quality, docs (Week 5)
| # | Branch | Owner | Done when |
|---|---|---|---|
| 5.1 | `feature/bi/metabase-dashboard` | C | KPIs from 6.4 visible, dashboard exported |
| 5.2 | `feature/tests/pipeline-tests` | A, B | Unit tests for validation and transform functions; one end-to-end smoke script |
| 5.3 | `docs/report-and-demo` | all | README, runbook, report, PPT, screenshots |
| 5.4 | `chore/final-cleanup` | C | Fresh clone → `make up` → demo works on all 3 machines |

Tag: `v1.0-final`

---

## 8. Airflow design

| DAG | Schedule | Flow |
|---|---|---|
| `generate_demo_data` | Manual | Create/refresh fake data (so the demo can be repeated) |
| `retail_batch_pipeline` | Daily | See below |
| `retail_stream_microbatch` | Every 5 min | consume Redpanda → validate → bronze/silver → load `fact_sales` (online channel) |

```
retail_batch_pipeline
  start
   |-- [TaskGroup: sales]      extract -> validate -> to_silver
   |-- [TaskGroup: inventory]  extract -> validate -> to_silver     (run in parallel)
   |-- [TaskGroup: customers]  extract -> validate -> to_silver
   v
  quality_gate  (fail or branch if reject % > threshold)
   v
  load_dimensions -> load_facts -> refresh_marts -> write_dq_summary
   v
  end
```

**Airflow rules for the team:**
- Every task is **idempotent** and uses the run date (`ds`) so it can be re-run.
- Set `retries=2`, `retry_delay=2min`, and an `on_failure_callback` that logs a clear message.
- **Keep DAG files thin:** they only call functions from `src/retailpulse/`. No business logic inside DAGs.
- Spark runs in **local mode inside the Airflow image** (no separate Spark cluster). This keeps it light for laptops. Mention the trade-off (a real cluster would be used in production) in the report.
- Pin one Airflow version and check its docs for import paths. Airflow 3.x changed some imports compared with 2.x (for example the `airflow.sdk` module), so follow the docs for the version you pin.

---

## 9. Timeline

| Week | Goal | Tag |
|---|---|---|
| 1 | Environment on all 3 machines, repo rules, contracts | `v0.1-foundation` |
| 2 | Generator and batch ingestion; start validation | `v0.2-ingestion` |
| 3 | Silver layer and warehouse | `v0.3-silver`, `v0.4-warehouse` |
| 4 | Streaming and Airflow DAGs | `v0.5-orchestrated` |
| 5 | Dashboard, tests, report, PPT, rehearsal | `v1.0-final` |

Hold a 15-minute sync every 2 to 3 days: what I merged, what I'm building, what's blocking me.

---

## 10. Risks and fixes

| Risk | Fix |
|---|---|
| Laptop runs out of RAM | Compose profiles; stop services you are not using; limit WSL memory |
| Image does not run on Mac ARM | Only multi-arch images; test in Week 1 |
| Windows line-ending errors | `.gitattributes`, work inside WSL |
| Merge conflicts in compose/DAGs | Separate files per area; one owner per file |
| Airflow import errors between versions | Pin the version, read its docs, don't copy old tutorials blindly |
| Streaming too complex | Fallback: producer writes to Redpanda, consumer is a simple script triggered by Airflow every 5 minutes |
| Demo fails on the day | Tag `v1.0-final`, screenshots in the PPT, and a pre-run dashboard |

---

## 11. Prompts (copy-paste into your AI assistant)

**How to use:** start every new AI chat with the **Master prompt**, then paste the feature prompt. Replace the text in `[brackets]`. Always read, run and test the output before committing.

### 11.1 Master context prompt (paste first, every time)

```text
I'm building "RetailPulse", a college data-engineering project: an end-to-end batch + streaming
retail data pipeline, built by 3 students on Windows (WSL2) and Apple-Silicon/Intel Mac laptops.

Stack: Docker Compose, Apache Airflow (pinned version), PostgreSQL (source DB + warehouse DB),
MinIO (S3-compatible lake, Parquet), PySpark in local mode, Redpanda (Kafka API), Python 3.11,
Metabase. No GPU, no cloud accounts.

Architecture: sources (sales Postgres DB, inventory CSVs, customer JSON, online-order events in
Redpanda) -> ingestion -> bronze (raw) -> validation (bad rows to quarantine with reason) ->
silver (clean) -> warehouse star schema (dim_date, dim_product, dim_customer, dim_store,
fact_sales, fact_inventory + marts) -> Metabase. Airflow schedules everything.

Repo layout: dags/ (thin, no logic), src/retailpulse/{common,generators,ingestion,validation,
transform,warehouse}, sql/{ddl,load,marts}, tests/, docs/.

Rules for all code you write:
- Must run inside Docker on both amd64 and arm64; use pinned versions and multi-arch images only.
- Config from environment variables (.env), never hard-coded secrets or absolute paths.
- Idempotent: re-running the same date must not create duplicates.
- Python: type hints, small functions, logging (no print), docstrings, a short pytest test.
- Keep it simple and readable; 3 students must be able to explain every line in a viva.
- After the code, give: (1) how to run it, (2) how to test it, (3) a 5-line plain-English
  explanation, (4) the files you created/changed.
If anything is ambiguous, state your assumption briefly and continue instead of asking many questions.
```

### 11.2 Member A: Ingestion & Streaming

**A1: Data generator (`feature/ingestion/data-generator`)**
```text
Write src/retailpulse/generators/ that creates realistic retail demo data with Faker and a fixed seed:
- Postgres source DB tables: stores(5), orders, order_items (20,000+ orders over 90 days)
- ~50 products across 6 categories (CSV master file) and ~1,000 customers (JSON file)
- daily inventory CSV files (product_id, store_id, stock_qty, snapshot_date), one file per day
Inject configurable defects (default ~3-5%): null customer_id, negative quantity, future dates,
duplicate order_ids, mixed date formats in inventory CSVs, duplicate customers, bad emails.
Provide a CLI: python -m retailpulse.generators --days 90 --defect-rate 0.04
Include the SQL to create the source tables and a pytest that checks counts and that defects exist.
```

**A2: Batch extractors (`feature/ingestion/sales-extract`, `inventory-csv`, `customer-json`)**
```text
Write src/retailpulse/ingestion/batch/ with three extractors: sales (Postgres -> bronze), inventory
(CSV folder -> bronze), customers (JSON -> bronze). Each takes a run date, reads the source for that
date, adds columns _ingested_at, _source_file/_batch_id, and writes Parquet to MinIO at
bronze/<source>/dt=YYYY-MM-DD/ using the S3 API. Requirements: idempotent (overwrite that date's
partition), skip already-processed CSV files, log row counts in/out, raise clear errors for missing
sources. Use PySpark in local mode or pandas + pyarrow; state which and why. Add a pytest with a tiny
fixture.
```

**A3: Streaming producer (`feature/streaming/order-producer`)**
```text
Write a Python producer (kafka-python or confluent-kafka; choose the one with a reliable arm64 wheel)
that publishes simulated online-store order events as JSON to the Redpanda topic orders_stream at a
configurable rate. Event fields: event_id, order_id, customer_id, product_id, quantity, unit_price,
event_time. Make ~3% of events invalid on purpose (malformed JSON, missing fields, negative quantity)
and ~2% late (old event_time). Config via env vars, graceful shutdown, logging. Include the
docker-compose.streaming.yml snippet to run it as a service under profile "streaming".
```

**A4: Streaming consumer + DLQ (`feature/streaming/consumer-dlq`)**
```text
Write a consumer for topic orders_stream that reads up to N messages or T seconds (micro-batch, so
Airflow can call it every 5 minutes), validates each event (required fields, types, quantity > 0,
price > 0), writes valid events to bronze Parquet in MinIO under bronze/online_orders/dt=.../, and sends
invalid ones to a dead-letter topic orders_dlq and to the quarantine table with the reason. Commit
offsets only after a successful write. Return counts (consumed, valid, invalid). Adapt the approach
from my earlier RabbitMQ/Redpanda lab. Add a test for the validation function.
```

### 11.3 Member B: Storage, Validation & Transformation

**B1: Lake layout + helpers (`feature/storage/lake-layout`, `feature/common/config-logging`)**
```text
Create src/retailpulse/common/ with: config.py (reads env vars with defaults and validation),
logging.py (consistent format), db.py (Postgres connection helper using SQLAlchemy/psycopg), and
lake.py (MinIO client helpers: read/write Parquet, list partitions, path builder for
<layer>/<source>/dt=YYYY-MM-DD/). Also write docs/lake_layout.md explaining the bronze/silver/
quarantine folder convention. Add small pytest tests.
```

**B2: Validation engine (`feature/validation/rule-engine`)**
```text
Design a simple, readable validation engine in src/retailpulse/validation/. Rules are defined per
source in a YAML or Python config (not_null, range, regex/format, unique, allowed_values,
referential). A function validate(df, rules) returns (good_df, bad_df) where bad_df has extra
columns rule_failed and reason; a row failing several rules records all of them. Work with Spark
DataFrames. Include rule configs for sales, inventory, customers matching these defects: null
customer_id, quantity <= 0, future date, bad email, stock < 0, duplicate order lines. Add pytest
cases for every rule type.
```

**B3: Quarantine + quality report (`feature/validation/quarantine-and-report`)**
```text
Add to src/retailpulse/validation/: (1) a function that writes bad rows to quarantine (MinIO
Parquet and a Postgres table dq.quarantine with source, rule_failed, reason, batch_id, ingested_at,
raw_record JSON), (2) a function that writes a per-batch summary row to dq.summary (source,
batch_id, total, accepted, rejected, reject_pct, per-rule counts), and (3) a quality_gate(summary,
threshold) function that raises a clear exception when reject_pct exceeds the threshold. Include the
DDL for the dq schema and tests.
```

**B4: Silver transforms (`feature/transform/silver-*`)**
```text
Write PySpark bronze->silver jobs for sales, inventory and customers in src/retailpulse/transform/.
Steps per job: read bronze for a run date, call the validation engine, split good/bad, standardize
types and date formats, trim/normalize strings, dedupe on business keys keeping the latest record,
add _processed_at, write silver Parquet (overwrite that date's partition) and send bad rows to
quarantine. Log row counts at each step. Expose each job as a function main(run_date: str) so
Airflow can call it. Add a small local test with a few rows. Explain the logic in plain English.
```

### 11.4 Member C: Warehouse, Airflow, Dashboard, Infra

**C1: Repo scaffold + Compose (`feature/infra/*`)**
```text
Create the project scaffold: README skeleton, .gitignore (python, data, .env, parquet, logs),
.gitattributes (* text=auto eol=lf), .env.example, Makefile (up, down, logs, test, lint),
PULL_REQUEST_TEMPLATE.md with the Definition of Done checklist, and CHANGELOG.md.
Then write docker-compose.yml with profile "core": PostgreSQL (two databases: source_db and
warehouse_db created by an init script), MinIO (with a bucket-creation init step), and Airflow
(official docker-compose pattern for ONE pinned version, LocalExecutor, own metadata DB). Add a custom
Dockerfile for Airflow that installs OpenJDK, pyspark and requirements.txt, and mounts dags/ and src/
with PYTHONPATH set. All images must be multi-arch (amd64 + arm64) and pinned. List every port and
the default credentials in the README. Tell me how to verify it works on Windows WSL2 and macOS.
```

**C2: Warehouse DDL (`feature/warehouse/ddl-star-schema`)**
```text
Write SQL (sql/ddl/) for a PostgreSQL star schema in schema dw: dim_date (generate 2 years),
dim_product, dim_customer, dim_store, fact_sales, fact_inventory, using surrogate keys, foreign keys,
and indexes on fact foreign keys. Add a "channel" column (in_store/online) on fact_sales.
Also create views mart_daily_revenue, mart_top_products, mart_low_stock (stock below a threshold),
and mart_dq_summary (from dq.summary). Provide an init script that runs the DDL idempotently
(CREATE ... IF NOT EXISTS) and a short explanation of the design choices.
```

**C3: Warehouse loads (`feature/warehouse/load-dimensions`, `load-facts`)**
```text
Write the warehouse load step reading silver Parquet from MinIO into Postgres for a given run date.
Dimensions: upsert with INSERT ... ON CONFLICT (type-1 is fine; mention what SCD type-2 would add).
Facts: look up surrogate keys, load fact_sales and fact_inventory for that date using delete-and-
reload by date_key so re-runs never duplicate. Use a staging table then a set-based SQL statement.
Rows that fail a key lookup go to dq.quarantine with reason "missing dimension". Expose
main(run_date) functions, log counts, and add a test that runs the load twice and checks that row
counts do not change.
```

**C4: Batch DAG (`feature/airflow/batch-dag`)**
```text
Write dags/retail_batch_pipeline.py for Airflow [VERSION], using the TaskFlow API and TaskGroups.
Flow: start -> three parallel TaskGroups (sales, inventory, customers), each extract -> validate ->
to_silver -> quality_gate -> load_dimensions -> load_facts -> refresh_marts -> write_dq_summary -> end.
Each task only calls functions from src/retailpulse (import them; no logic in the DAG). Daily schedule,
catchup=False, retries=2, retry_delay=2 minutes, on_failure_callback that logs a readable message,
pass the logical date (ds) into every task, tags and doc_md. Use the correct imports for my pinned
Airflow version, and tell me which docs page to check. Explain how to trigger and read it in the UI.
```

**C5: Streaming DAG (`feature/airflow/stream-dag`)**
```text
Write dags/retail_stream_microbatch.py running every 5 minutes with max_active_runs=1: task 1 runs
the consumer micro-batch (from src/retailpulse/ingestion/stream), task 2 runs silver validation for
the online_orders micro-batch, task 3 loads them into fact_sales with channel='online' (idempotent by
event_id), task 4 logs counts. Add a short-circuit so the later tasks skip when no new messages
arrived. Same standards as the batch DAG.
```

**C6: Dashboard (`feature/bi/metabase-dashboard`)**
```text
Add compose.bi.yml with Metabase (pinned multi-arch image, its own app database, not the warehouse).
Then give me the SQL for each dashboard card using the dw schema: revenue trend by day, top 10
products, sales by category, store performance, online vs in-store share, low-stock alerts, and a
data-quality scorecard (accepted vs rejected by source from dq.summary). Explain step by step how to
connect Metabase to warehouse_db, create the questions, assemble the dashboard and export/screenshot it.
```

**C7: Tests / smoke script (`feature/tests/pipeline-tests`)**
```text
Write an end-to-end smoke test script (bash or Python) that: starts the stack, generates one day of
data, triggers retail_batch_pipeline through the Airflow CLI/API, waits for success, then runs SQL
assertions (row counts > 0, no duplicate keys in fact_sales, reject_pct below the threshold) and
prints PASS/FAIL. Keep it simple and document how to run it with `make smoke`.
```

### 11.5 Shared prompts (any member, any time)

**Code review (before opening a PR)**
```text
Review the following code as a senior data engineer. Check: correctness, idempotency, error handling,
logging, Windows/WSL + Apple-Silicon compatibility, hard-coded paths/secrets, missing tests, and
anything a student could not easily explain. List problems by severity and show fixed code for the
top 3. [paste code]
```

**Debug**
```text
This fails in my Docker setup (OS: [Windows WSL2 / macOS]). Give the most likely causes ranked, the
exact commands to confirm each, and the fix. Error + relevant logs: [paste]. docker compose logs
for the service: [paste]. What I changed last: [describe].
```

**Viva preparation**
```text
Explain this module as if to an examiner, in simple language: what problem it solves, the data flow
step by step, why each design choice was made, what could go wrong, and 8 likely viva questions with
short model answers. Code: [paste]
```

**Docs / report section**
```text
Write the [Methodology / Implementation / Results] section for my project report from the notes
below. Use a clear academic tone, keep technical accuracy, do not invent numbers or results, and mark
anything I must fill in as [TO FILL]. Notes: [paste]
```

**Commit + PR description**
```text
Given this diff, write (1) a conventional commit message, (2) a PR description with: what changed,
why, how to run/test it, and a Definition-of-Done checklist, (3) a CHANGELOG line. Diff: [paste]
```

---

## 12. Final checklist before submission

- [ ] Fresh clone → `make up` works on all 3 laptops
- [ ] All 3 DAGs run; at least one failure and recovery demonstrated (e.g., the quality gate tripping)
- [ ] Dashboard shows all KPIs and the data-quality scorecard
- [ ] Every phase tagged; `main` clean; no secrets or data in the repo
- [ ] Report, PPT, README, `docs/` finished; screenshots saved (Airflow graph and runs, MinIO buckets, warehouse tables, dashboard, quarantine and DQ summary)
- [ ] Every member rehearsed explaining their own stage and one other member's stage
