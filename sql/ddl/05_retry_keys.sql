-- Apply to warehouse_db after the base DQ/DW schemas, including existing volumes.
ALTER TABLE dq.quarantine ADD COLUMN IF NOT EXISTS record_hash TEXT
    GENERATED ALWAYS AS (md5(raw_record::text || rule_failed)) STORED;
DELETE FROM dq.quarantine a USING dq.quarantine b
WHERE a.source=b.source AND a.batch_id=b.batch_id AND a.record_hash=b.record_hash AND a.id<b.id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_quarantine_batch_record
    ON dq.quarantine(source, batch_id, record_hash);
DELETE FROM dq.summary a USING dq.summary b
WHERE a.source=b.source AND a.batch_id=b.batch_id AND a.id<b.id;
CREATE UNIQUE INDEX IF NOT EXISTS uq_summary_batch ON dq.summary(source, batch_id);
ALTER TABLE dw.fact_sales ADD COLUMN IF NOT EXISTS event_id TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_sales_event ON dw.fact_sales(event_id);
