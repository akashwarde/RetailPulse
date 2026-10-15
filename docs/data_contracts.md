# Batch data contracts

All ingestion functions take a logical ISO `run_date` and overwrite
`bronze/<source>/dt=<run_date>/data.parquet`. Raw business values stay strings
so malformed values reach validation. `_source_file`, `_ingested_at`, and a
stable `_batch_id=<source>_<date>` accompany each row.

| Source | Input | Business key | Required validation |
|---|---|---|---|
| Sales | PostgreSQL `raw_sales`, selected by `source_date` | order_id + order_item_id | Customer/product/store IDs; integral positive quantity; finite positive price; valid timestamp no later than now; product reference exists |
| Inventory | `inventory/inventory_YYYY-MM-DD*.csv` | product_id + store_id + normalized snapshot_date | Required IDs; integral nonnegative stock; valid nonfuture date; product reference exists |
| Customers | `customers.json` array of objects | customer_id | Required customer ID, first name, city; valid email; normalize signup date and strings |
| Products | `products.csv` | product_id | Unique positive ID; name/category; finite positive price |
| Stores | `stores.csv` | store_id | Unique positive ID; store name/city |

`raw_sales` is a permissive landing table. The generator also populates normalized
`customers`, `orders`, and `order_items` with their clean counterparts. Operational
constraints remain intact; deliberately invalid copies are placed in raw staging.
`source_date` retains the intended partition when an injected future timestamp
would otherwise move the defective row out of the extraction window.

Inventory headers are trimmed, case-normalized and accept spaces instead of
underscores. Explicit aliases cover `productid`, `storeid`, `stock_quantity`,
and `date`. Missing, duplicate, unknown headers or extra row fields raise errors.
Unchanged files are skipped only if their fingerprint and bronze object exist.

Normalization repairs ISO, day/month/year, and month/day/year dates in that
priority order. Ambiguous slash dates use day/month/year. Invalid casts/dates
must be rejected, not silently published. Duplicate survivors are chosen by
validity, configured latest timestamp, and a stable payload tie-breaker.

Rejected rows are saved to MinIO and `dq.quarantine`, with source, failed rules,
reason, logical batch ID, ingestion timestamp (database default), and raw JSON.
`dq.summary` has one row per source/logical batch. Re-running a corrected batch
replaces quarantine and summary results, including an empty rejection set.
More than 5% rejected rows raises `QualityGateError` before silver publication.

Product/store files are trusted masters after their explicit validation and are
published directly to silver. Run `extract_masters` before sales/inventory
transforms and dimension loading. Missing required partitions and service errors
fail the caller rather than returning a successful zero-row load.

Warehouse batch inputs represent complete date snapshots. Online rows represent
increments and require `event_id`. Online replay is ignored by its unique key;
late events retain their actual event date. Fact-load lookup failures are written
to PostgreSQL quarantine atomically with accepted facts.

## Streaming contract (`orders_stream`)
Required: `event_id`, `order_id`, `customer_id`, `product_id`, positive integral `quantity`, positive finite `unit_price`,
parseable `event_time`. Valid events go to `bronze/online_orders/dt=<event date>/part-<batch>.parquet` (file name derives from
Kafka offsets, so replays overwrite instead of duplicating). Invalid events go to `quarantine/online_orders/...`, `dq.quarantine`
and the `orders_dlq` topic (envelope: reason + original payload). Late events are accepted. Silver de-duplicates on `event_id`;
the warehouse loads through `load_fact_sales(channel="online")`, which never deletes older facts.

## Quality gates
Batch: reject % above `QUALITY_GATE_THRESHOLD` (default 5) raises `QualityGateError`; the summary row is still persisted.
Stream: above `STREAM_QUALITY_THRESHOLD` (15) on at least `STREAM_GATE_MIN_MESSAGES` (50) messages fails `quality_gate`
after the valid events have already been loaded.

