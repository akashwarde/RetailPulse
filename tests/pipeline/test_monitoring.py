"""Failure logging, webhook alerts and the streaming quality gate."""

from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest

from retailpulse import monitoring
from retailpulse.monitoring import StreamQualityGateError, format_failure_message, stream_quality_gate
from retailpulse.validation.quarantine import QualityGateError


def ctx(exc):
    ti = SimpleNamespace(dag_id="retail_batch_pipeline", task_id="sales.to_silver", try_number=3)
    return {"task_instance": ti, "run_id": "manual__x", "ds": "2026-03-10", "exception": exc, "dag": SimpleNamespace(dag_id="retail_batch_pipeline")}


def test_failure_message_names_dag_task_and_error():
    msg = format_failure_message(ctx(ValueError("boom")))
    assert "TASK FAILED" in msg and "retail_batch_pipeline" in msg and "sales.to_silver" in msg and "boom" in msg


def test_gate_failure_is_labelled():
    assert "QUALITY GATE FAILED" in format_failure_message(ctx(QualityGateError("5% exceeded")))


def test_callback_records_event_logs_file_and_posts_webhook(tmp_path, monkeypatch):
    log = tmp_path / "alerts.log"
    monkeypatch.setenv("ALERT_LOG_PATH", str(log))
    with mock.patch.object(monitoring, "record_event", return_value=True) as rec, \
            mock.patch.object(monitoring, "post_webhook", return_value=True) as hook:
        monitoring.on_failure_callback(ctx(QualityGateError("too many rejects")))
    assert rec.call_args.kwargs["status"] == "GATE_FAILED"
    hook.assert_called_once()
    assert "too many rejects" in log.read_text()


def test_record_event_never_raises_when_db_is_down():
    with mock.patch.object(monitoring, "get_db_connection", side_effect=RuntimeError("db down")):
        assert monitoring.record_event("dag", "FAILED", "msg") is False


def test_webhook_is_skipped_without_url(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert monitoring.post_webhook("hello") is False


def test_webhook_failure_does_not_raise(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/none")
    assert monitoring.post_webhook("hello") is False


class TestStreamGate:
    def test_passes_under_threshold(self):
        assert stream_quality_gate({"consumed": 200, "invalid": 6, "reject_pct": 3.0})["passed"]

    def test_fails_over_threshold(self):
        with pytest.raises(StreamQualityGateError, match="Stream quality gate FAILED"):
            stream_quality_gate({"consumed": 200, "invalid": 60, "reject_pct": 30.0, "batch_id": "b"})

    def test_tiny_batches_are_ignored(self):
        assert stream_quality_gate({"consumed": 4, "invalid": 3, "reject_pct": 75.0})["passed"]

    def test_threshold_is_configurable(self, monkeypatch):
        monkeypatch.setenv("STREAM_QUALITY_THRESHOLD", "2")
        with pytest.raises(StreamQualityGateError):
            stream_quality_gate({"consumed": 200, "invalid": 10, "reject_pct": 5.0})


def test_batch_threshold_env(monkeypatch):
    from retailpulse.validation.quarantine import get_quality_threshold

    monkeypatch.setenv("QUALITY_GATE_THRESHOLD", "12.5")
    assert get_quality_threshold() == 12.5
    monkeypatch.setenv("QUALITY_GATE_THRESHOLD", "abc")
    with pytest.raises(ValueError):
        get_quality_threshold()
