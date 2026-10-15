-- Operational log written by the Airflow failure callback and the DQ summary
-- task. It gives the dashboard (and the viva) a durable record of what failed,
-- when, and why -- independent of Airflow's own metadata database.
CREATE TABLE IF NOT EXISTS dq.pipeline_events (
    id          BIGSERIAL PRIMARY KEY,
    dag_id      VARCHAR(200) NOT NULL,
    task_id     VARCHAR(200),
    run_id      VARCHAR(250),
    run_date    DATE,
    status      VARCHAR(30) NOT NULL,      -- SUCCESS | FAILED | GATE_FAILED | INFO
    message     TEXT,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_pipeline_events_created ON dq.pipeline_events(created_at);
CREATE INDEX IF NOT EXISTS idx_pipeline_events_status  ON dq.pipeline_events(status);
