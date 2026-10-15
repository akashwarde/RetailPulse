# Report outline
1 Problem & goals · 2 Architecture (diagram) · 3 Data sources & defect injection · 4 Medallion lake · 5 Validation & quarantine · 6 Star schema & marts · 7 Orchestration · 8 Streaming & DLQ · 9 Idempotency & failure handling · 10 Testing (255 unit + 18 smoke) · 11 Dashboard · 12 Limitations & future work (SCD2, lineage, CI).

# Presentation (10–12 slides)
Problem → architecture → live `make demo` → quarantine & gate failure (`make gate-demo`) → recovery → streaming + DLQ → dashboard → tests → lessons learned.

# Screenshot checklist
Airflow graph (batch) · successful run · failed gate run · stream DAG · MinIO bucket tree · `dq.summary` query · Metabase dashboard · Redpanda DLQ topic · test output.
