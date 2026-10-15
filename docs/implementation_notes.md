# Additional implementation fixes

Changes were implemented on `main`, based on original remote commit `0bab518`.
The repository's default branch is `master`; integration uses a pull request
from `main` into that branch.

Implemented in this change:

- Configurable, deterministic multi-source generation, normalized PostgreSQL
  source tables plus raw defect staging, customer JSON, inventory CSVs, and
  shared master ID ranges.
- Sales, inventory, and customer extractors plus product/store publication.
- Default DQ connections, retry-safe persistence, corrected rule counts,
  validation normalization, cast checks, and deterministic duplicate selection.
- Event-id online upserts, real warehouse quarantine writes, date replacement
  including empty corrected snapshots, and explicit failure on missing inputs.
- Container application variables, SQL/schema deployment, initialization order,
  architecture-independent Java path, pinned development dependencies, and
  direct batch/test entrypoints.

Still pending from the separate Phase 4/5 work:

- Stream consumer, DLQ processing and safe offset commits.
- Three Airflow DAGs, schedules, failure callbacks and notifications.
- Metabase services, cards, dashboard export, and screenshots.
- Airflow-driven end-to-end smoke demonstration, project report/PPT/runbook,
  and verification on all three laptops.

The direct `python -m retailpulse.pipeline DATE` command exposes batch behavior
for manual validation. It does not create or schedule Airflow DAGs.

## Validation

Verified on 15 October 2026 using Linux/amd64 Docker through WSL Ubuntu:

- The Airflow Docker image builds with Python 3.12, Java 17, and PySpark 3.5.4.
- PostgreSQL, MinIO, bucket initialization, directory permissions, application
  schema initialization, and Airflow metadata/user initialization succeed.
- Repeating initialization against existing volumes succeeds.
- The Airflow webserver and scheduler both report healthy; `/health` reports a
  healthy metadatabase and scheduler heartbeat. Metadata connection settings are
  resolved at service startup, with a CLI/health-check fallback for Docker exec.
- **185 tests passed, 0 failed, 0 skipped**, in 110.18 seconds inside the image.
  These include real PostgreSQL/MinIO/Spark integration and the complete direct
  batch workflow run twice with identical fact counts, revenue, and DQ counts.
- Online replay, successive micro-batches, persisted lookup failures, corrected
  empty snapshots, and failed-write rollback are exercised against PostgreSQL.
- `pip check` reports no broken requirements; Compose configuration validates.

Integration tests use unique disposable databases through
`RETAILPULSE_TEST_DSN`; they never assume the warehouse contains particular rows.
`RETAILPULSE_TEST_MINIO=1` enables the full batch test with a disposable bucket.
The pinned Spark/pandas combination emits deprecation warnings; the test logger
also emits a Py4J logging message at interpreter shutdown after the successful
test result. The tests return exit code zero.

Native Windows Spark worker checks were unreliable, so the authoritative results
above are from the intended Docker runtime. Apple Silicon and the other laptops
still require their own verification; the separate Airflow/streaming/dashboard
workflow is not implemented or certified by this change.

The pre-change audit report saved outside this repository describes the original
commit and is historical; it is not a review of these modifications.

See [teammate_handoff.md](teammate_handoff.md) for callable APIs, reserved modules,
and shared files that need reconciliation with the remaining work.
