# Runbook

**Start:** `make env && make up`, wait for `make wait`. **Stop:** `make down` (`make clean` also removes volumes).

| Symptom | Check / fix |
|---|---|
| DAG failed with QualityGateError | `SELECT * FROM dq.summary ORDER BY 1 DESC;` and `dq.quarantine` for the failing rule; fix source, re-run (`make batch`) – safe to repeat |
| Task failed after retries | Airflow task log; `dq.pipeline_events`; `alerts.log` |
| Stream DAG has no data | `make stream-demo` is the `streaming` profile up? check Redpanda console |
| Messages in `orders_dlq` | inspect envelope (reason + original payload), correct producer |
| Dashboard empty | run `make demo` first; `make dashboard` is idempotent |
| Spark JVM errors on M-series Mac | rebuild image (`docker compose build`); JAVA_HOME is `/usr/lib/jvm/java-17` |
| Out of memory | lower the `spark` pool size or `docker compose --profile core` only (`make up-lite`) |

Backfill: `make backfill DAYS=7`. Slack/Teams alerts: set `ALERT_WEBHOOK_URL` in `.env`.

Windows: use Docker Desktop with WSL2 and run `make` inside WSL.
Not tested live: Docker stack, real Redpanda, Metabase provisioning – report issues from first run.
