"""PostgreSQL connection helpers using psycopg2."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

import psycopg2

if TYPE_CHECKING:
    from psycopg2.extensions import connection as PgConnection

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.logging import get_logger

_log = get_logger("common.db")


def get_connection(
    database: str = "source",
    config: AppConfig | None = None,
) -> PgConnection:
    """Open a psycopg2 connection to a project database.

    Args:
        database: Either ``"source"`` (default) or ``"warehouse"``.
        config: Optional :class:`AppConfig`; loaded from the environment
            if not provided.

    Returns:
        An open :class:`psycopg2.extensions.connection`.

    Raises:
        ValueError: If *database* is not ``"source"`` or ``"warehouse"``.
        psycopg2.OperationalError: If the connection attempt fails.
    """
    if config is None:
        config = get_config()

    dsn = config.postgres.dsn(database)
    _log.info(
        "Connecting to PostgreSQL database=%s host=%s",
        database,
        config.postgres.host,
    )
    conn = psycopg2.connect(dsn)
    _log.info("Connected to PostgreSQL database=%s", database)
    return conn


@contextmanager
def get_db_connection(
    database: str = "source",
    config: AppConfig | None = None,
) -> Generator[PgConnection, None, None]:
    """Context manager for PostgreSQL connection with reliable closing and cleanup.

    Yields:
        An open :class:`psycopg2.extensions.connection`.
    """
    conn = get_connection(database=database, config=config)
    try:
        yield conn
    finally:
        if not conn.closed:
            conn.close()
            _log.info("Closed PostgreSQL connection to %s", database)
