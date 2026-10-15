# RetailPulse Data Lake Layout & Storage Conventions

This document specifies the storage layout, directory conventions, and medallion architecture for the RetailPulse S3-compatible data lake hosted on **MinIO**.

---

## 1. Storage Architecture Overview

RetailPulse uses MinIO as an S3-compatible object store for raw, cleansed, and rejected dataset records. All data is serialized using **Apache Parquet**, providing efficient columnar storage, schema preservation, and fast queries for PySpark and PyArrow jobs.

- **Default Bucket:** `retailpulse` (configured via `MINIO_BUCKET` in `.env`)
- **Storage Format:** Apache Parquet (`.parquet`)
- **Default Object Name:** `data.parquet` (or partition chunks like `part-0000.parquet`)
- **Partitioning Scheme:** Hive-style date partitions (`dt=YYYY-MM-DD`)

---

## 2. Directory Path Convention

All objects stored in the lake follow a strict hierarchical key structure:

```text
<layer>/<source>/dt=<YYYY-MM-DD>/<filename>
```

### Path Components

| Component | Format / Constraint | Description | Example |
|---|---|---|---|
| `<layer>` | `bronze` \| `silver` \| `quarantine` | Medallion storage tier | `bronze` |
| `<source>` | Lowercase alphanumeric (`[a-z_]+`) | Originating data domain or stream | `sales`, `inventory`, `customers`, `orders_stream` |
| `dt=<YYYY-MM-DD>` | Hive partition token with ISO 8601 date | Business extraction or snapshot date | `dt=2025-01-15` |
| `<filename>` | String ending in `.parquet` | Physical file name (defaults to `data.parquet`) | `data.parquet` |

---

## 3. Medallion Layers

The lake is organized into three allowed tiers defined in `src/retailpulse/common/lake.py` (`_ALLOWED_LAYERS`):

### 3.1 Bronze (`bronze/`)
* **Purpose:** Raw data landing zone preserving source fidelity.
* **Write Semantics:** Append-only per batch/ingestion run.
* **Schema:** Source schema as extracted, with minimal or no structural alterations.
* **Retention:** Replayable history for backfills and auditing.
* **Path Example:**
  ```text
  bronze/sales/dt=2025-01-15/data.parquet
  bronze/inventory/dt=2025-01-15/data.parquet
  bronze/customers/dt=2025-01-15/data.parquet
  bronze/orders_stream/dt=2025-01-15/part-0000.parquet
  ```

### 3.2 Silver (`silver/`)
* **Purpose:** Cleansed, validated, and enriched analytical dataset.
* **Write Semantics:** Idempotent overwrite per partition date (`dt=YYYY-MM-DD`).
* **Transformations Applied:**
  * Deduplication on primary / business keys (keeping the latest valid record).
  * Strict schema casting and type standardization (e.g. numeric types, ISO timestamps).
  * String whitespace stripping and value normalization.
  * Ingestion metadata appended (e.g., `_processed_at`).
* **Path Example:**
  ```text
  silver/sales/dt=2025-01-15/data.parquet
  silver/inventory/dt=2025-01-15/data.parquet
  silver/customers/dt=2025-01-15/data.parquet
  ```

### 3.3 Quarantine (`quarantine/`)
* **Purpose:** Dead-letter and defect containment for rows failing validation contracts.
* **Write Semantics:** Append per batch run.
* **Characteristics:**
  * Captures records that violate rules (e.g. null keys, negative quantity/price, invalid email syntax, future dates).
  * Includes audit metadata: `rule_failed`, `reason`, `batch_id`, `ingested_at`, and raw record payload.
  * Feeds the Data Quality scorecard on the dashboard and quality gate alert thresholds.
* **Path Example:**
  ```text
  quarantine/sales/dt=2025-01-15/data.parquet
  quarantine/inventory/dt=2025-01-15/data.parquet
  quarantine/customers/dt=2025-01-15/data.parquet
  ```

---

## 4. Source Domains

The lake ingests data from four primary sources:

| Source Name | Ingestion Type | Source System | Typical Data Content |
|---|---|---|---|
| `sales` | Batch (Daily) | PostgreSQL (`source_db`) | In-store transactions (`orders`, `order_items`, `stores`) |
| `inventory` | Batch (Daily drops) | CSV files | Daily warehouse stock levels (`product_id`, `store_id`, `stock_qty`, `snapshot_date`) |
| `customers` | Batch (Daily/Periodic) | JSON / API | Customer master profiles (`customer_id`, `name`, `email`, `city`) |
| `orders_stream` | Streaming (Micro-batch) | Redpanda topic | Online order events |

---

## 5. Python Lake Utilities (`src/retailpulse/common/lake.py`)

All interaction with the lake layout is standardized through the shared `retailpulse.common.lake` module.

### 5.1 Building Lake Paths

Use `build_lake_path()` to construct standard object keys:

```python
from retailpulse.common.lake import build_lake_path

# Standard bronze path (default filename: "data.parquet")
path_sales = build_lake_path("bronze", "sales", "2025-01-15")
# -> "bronze/sales/dt=2025-01-15/data.parquet"

# Silver path with custom partition filename
path_silver = build_lake_path(
    "silver", "inventory", "2025-03-01", filename="part-0000.parquet"
)
# -> "silver/inventory/dt=2025-03-01/part-0000.parquet"

# Quarantine path
path_quarantine = build_lake_path("quarantine", "customers", "2025-06-30")
# -> "quarantine/customers/dt=2025-06-30/data.parquet"
```

> **Note:** Only `"bronze"`, `"silver"`, and `"quarantine"` are permitted as the `layer` argument. Invalid layers raise a `ValueError`.

### 5.2 Listing Partitions

Use `list_partitions()` to discover available dates under `<layer>/<source>/`:

```python
from retailpulse.common.config import get_config
from retailpulse.common.lake import get_minio_client, list_partitions

config = get_config()
client = get_minio_client(config)

# Returns sorted list of date strings, e.g. ["2025-01-14", "2025-01-15"]
partitions = list_partitions(client, config.minio.bucket, "bronze", "sales")
```

### 5.3 Reading and Writing Parquet Tables

Use `write_parquet()` and `read_parquet()` with PyArrow tables:

```python
import pyarrow as pa
from retailpulse.common.config import get_config
from retailpulse.common.lake import (
    build_lake_path,
    get_minio_client,
    read_parquet,
    write_parquet,
)

config = get_config()
client = get_minio_client(config)
bucket = config.minio.bucket

# Write table to bronze
table = pa.table({"order_id": [101, 102], "amount": [499.50, 1250.00]})
object_key = build_lake_path("bronze", "sales", "2025-01-15")
write_parquet(client, bucket, object_key, table)

# Read table from bronze
loaded_table = read_parquet(client, bucket, object_key)
print(f"Loaded {loaded_table.num_rows} rows from {object_key}")
```

---

## 6. Lake Layout Tree Example

```text
retailpulse/ (MinIO Bucket)
├── bronze/
│   ├── sales/
│   │   ├── dt=2025-01-14/
│   │   │   └── data.parquet
│   │   └── dt=2025-01-15/
│   │       └── data.parquet
│   ├── inventory/
│   │   └── dt=2025-01-15/
│   │       └── data.parquet
│   └── customers/
│       └── dt=2025-01-15/
│           └── data.parquet
├── silver/
│   ├── sales/
│   │   ├── dt=2025-01-14/
│   │   │   └── data.parquet
│   │   └── dt=2025-01-15/
│   │       └── data.parquet
│   ├── inventory/
│   │   └── dt=2025-01-15/
│   │       └── data.parquet
│   └── customers/
│       └── dt=2025-01-15/
│           └── data.parquet
└── quarantine/
    ├── sales/
    │   └── dt=2025-01-15/
    │       └── data.parquet
    ├── inventory/
    │   └── dt=2025-01-15/
    │       └── data.parquet
    └── customers/
        └── dt=2025-01-15/
            └── data.parquet
```
