# Changelog

## Unreleased

- Add deterministic multi-source demo data, PostgreSQL raw sales staging, three
  date-scoped batch extractors, and validated product/store master publication.
- Repair DQ connection calls, stable batch identities, retry-safe summaries and
  quarantine replacement, per-rule reporting, and validation/deduplication.
- Preserve online micro-batches with event-id replay protection; persist failed
  warehouse lookups; replace corrected/empty batch snapshots atomically.
- Wire application environment, SQL initialization, service dependencies, portable
  Java paths, and direct batch/test commands into the Compose setup.
- Add component and isolated PostgreSQL regression coverage and usage documentation.
