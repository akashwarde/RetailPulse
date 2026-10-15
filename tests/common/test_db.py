"""Tests for retailpulse.common.db."""

from unittest import mock

import pytest

from retailpulse.common.config import AppConfig, MinIOConfig, PostgresConfig
from retailpulse.common.db import get_connection, get_db_connection


def _make_config() -> AppConfig:
    """Return a test AppConfig with predictable values."""
    return AppConfig(
        postgres=PostgresConfig(
            host="testhost",
            port=5432,
            user="testuser",
            password="testpass",
            source_db="test_source",
            warehouse_db="test_warehouse",
        ),
        minio=MinIOConfig(access_key="k", secret_key="s"),
    )


class TestGetConnection:
    """Tests for get_connection()."""

    @mock.patch("retailpulse.common.db.psycopg2")
    def test_connects_to_source(self, mock_pg: mock.MagicMock) -> None:
        cfg = _make_config()
        conn = get_connection("source", config=cfg)

        mock_pg.connect.assert_called_once()
        dsn_arg = mock_pg.connect.call_args[0][0]
        assert "dbname=test_source" in dsn_arg
        assert "host=testhost" in dsn_arg
        assert conn is mock_pg.connect.return_value

    @mock.patch("retailpulse.common.db.psycopg2")
    def test_connects_to_warehouse(self, mock_pg: mock.MagicMock) -> None:
        cfg = _make_config()
        get_connection("warehouse", config=cfg)

        dsn_arg = mock_pg.connect.call_args[0][0]
        assert "dbname=test_warehouse" in dsn_arg

    def test_invalid_database_raises(self) -> None:
        cfg = _make_config()
        with pytest.raises(ValueError):
            get_connection("invalid", config=cfg)

    @mock.patch("retailpulse.common.db.psycopg2")
    def test_default_database_is_source(self, mock_pg: mock.MagicMock) -> None:
        cfg = _make_config()
        get_connection(config=cfg)

        dsn_arg = mock_pg.connect.call_args[0][0]
        assert "dbname=test_source" in dsn_arg

    @mock.patch("retailpulse.common.db.psycopg2")
    def test_get_db_connection_context_manager(self, mock_pg: mock.MagicMock) -> None:
        cfg = _make_config()
        mock_conn = mock.MagicMock()
        mock_conn.closed = False
        mock_pg.connect.return_value = mock_conn

        with get_db_connection("source", config=cfg) as conn:
            assert conn is mock_conn
            mock_conn.close.assert_not_called()

        mock_conn.close.assert_called_once()
