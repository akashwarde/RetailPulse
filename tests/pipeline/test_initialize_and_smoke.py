"""Unit tests for database bootstrapping and the smoke-test helpers (no real services needed)."""
import json
from types import SimpleNamespace
from unittest import mock

from retailpulse.common import initialize
from retailpulse import smoke


class _Cursor:
    def __init__(self, existing):
        self.existing, self.executed, self._row = set(existing), [], None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        text = query if isinstance(query, str) else str(query)
        self.executed.append(text)
        if "pg_database" in text:
            self._row = (1,) if params[0] in self.existing else None

    def fetchone(self):
        return self._row


def _fake_connect(existing):
    cur = _Cursor(existing)
    conn = mock.MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def _patch_config():
    cfg = SimpleNamespace(postgres=SimpleNamespace(warehouse_db="warehouse_db", dsn=lambda which: "dsn"))
    return mock.patch.object(initialize, "get_config", return_value=cfg)


def test_ensure_databases_creates_missing_ones():
    conn, cur = _fake_connect(existing=set())
    with _patch_config(), mock.patch.object(initialize.psycopg2, "connect", return_value=conn), \
            mock.patch.object(initialize, "sql", mock.MagicMock()):
        created = initialize.ensure_databases()
    assert created == ["warehouse_db", "airflow_db"]
    assert conn.autocommit is True
    conn.close.assert_called_once()


def test_ensure_databases_is_a_noop_when_present():
    conn, cur = _fake_connect(existing={"warehouse_db", "airflow_db"})
    with _patch_config(), mock.patch.object(initialize.psycopg2, "connect", return_value=conn):
        assert initialize.ensure_databases() == []
    assert not any("CREATE DATABASE" in q for q in cur.executed)


def _airflow_output(runs, indent=None):
    log = "[2026-10-15T18:11:05.855+0000] {__init__.py:43} INFO - Loaded API auth backend\n"
    return SimpleNamespace(stdout=log + json.dumps(runs, indent=indent))


def test_airflow_state_finds_run_among_log_noise():
    runs = [{"run_id": "other", "state": "running"}, {"run_id": "smoke_x", "state": "Success"}]
    for indent in (None, 2):
        with mock.patch.object(smoke.subprocess, "run", return_value=_airflow_output(runs, indent)):
            assert smoke.airflow_state("dag", "smoke_x") == "success"


def test_airflow_state_unknown_when_missing_or_garbled():
    with mock.patch.object(smoke.subprocess, "run", return_value=_airflow_output([])):
        assert smoke.airflow_state("dag", "nope") == "unknown"
    with mock.patch.object(smoke.subprocess, "run", return_value=SimpleNamespace(stdout="error: boom")):
        assert smoke.airflow_state("dag", "nope") == "unknown"


def test_compose_postgres_host_port_is_configurable():
    text = open("compose.yaml").read()
    assert "${POSTGRES_HOST_PORT:-5432}:5432" in text
    assert "POSTGRES_HOST_PORT" in open(".env.example").read()
