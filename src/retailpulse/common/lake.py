"""MinIO (S3-compatible) lake helpers for RetailPulse."""

from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from retailpulse.common.config import AppConfig, get_config
from retailpulse.common.logging import get_logger

_log = get_logger("common.lake")

_ALLOWED_LAYERS = {"bronze", "silver", "quarantine"}


def get_minio_client(config: AppConfig | None = None) -> Minio:
    """Create a MinIO client from configuration.

    Args:
        config: Optional :class:`AppConfig`; loaded from the environment
            if not provided.

    Returns:
        A configured :class:`minio.Minio` client.
    """
    if config is None:
        config = get_config()

    mc = config.minio
    _log.info("Creating MinIO client endpoint=%s", mc.endpoint)
    return Minio(
        mc.endpoint,
        access_key=mc.access_key,
        secret_key=mc.secret_key,
        secure=mc.secure,
    )


def build_lake_path(
    layer: str,
    source: str,
    dt: str,
    filename: str = "data.parquet",
) -> str:
    """Build a standardised lake object path.

    Convention: ``<layer>/<source>/dt=<YYYY-MM-DD>/<filename>``

    Args:
        layer: Data layer - ``"bronze"``, ``"silver"``, or ``"quarantine"``.
        source: Data source name, e.g. ``"sales"``, ``"inventory"``.
        dt: Partition date in ``YYYY-MM-DD`` format.
        filename: Object filename (default ``"data.parquet"``).

    Returns:
        The full object key,
        e.g. ``"bronze/sales/dt=2025-01-15/data.parquet"``.

    Raises:
        ValueError: If *layer* is not one of the allowed values.
    """
    if layer not in _ALLOWED_LAYERS:
        raise ValueError(
            f"layer must be one of {_ALLOWED_LAYERS}, got '{layer}'"
        )
    return f"{layer}/{source}/dt={dt}/{filename}"


def write_parquet(
    client: Minio,
    bucket: str,
    object_name: str,
    table: pa.Table,
) -> None:
    """Write a PyArrow Table as a Parquet object to MinIO.

    Args:
        client: An initialised :class:`minio.Minio` client.
        bucket: Target bucket name.
        object_name: Full object key (use :func:`build_lake_path`).
        table: The :class:`pyarrow.Table` to serialise and upload.
    """
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)
    size = buf.getbuffer().nbytes

    client.put_object(
        bucket, object_name, buf, length=size,
        content_type="application/octet-stream",
    )
    _log.info("Wrote %d bytes to s3://%s/%s", size, bucket, object_name)


def read_parquet(
    client: Minio,
    bucket: str,
    object_name: str,
) -> pa.Table:
    """Read a Parquet object from MinIO into a PyArrow Table.

    Args:
        client: An initialised :class:`minio.Minio` client.
        bucket: Source bucket name.
        object_name: Full object key.

    Returns:
        The data as a :class:`pyarrow.Table`.
    """
    _log.info("Reading s3://%s/%s", bucket, object_name)
    response = client.get_object(bucket, object_name)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    buf = io.BytesIO(data)
    return pq.read_table(buf)


def list_partitions(
    client: Minio,
    bucket: str,
    layer: str,
    source: str,
) -> list[str]:
    """List partition dates for a given layer and source.

    Scans prefixes matching ``<layer>/<source>/dt=<date>/``.

    Args:
        client: An initialised :class:`minio.Minio` client.
        bucket: Bucket name.
        layer: Data layer (e.g. ``"bronze"``).
        source: Data source (e.g. ``"sales"``).

    Returns:
        A sorted list of date strings found as partitions.
    """
    prefix = f"{layer}/{source}/dt="
    dates: set[str] = set()

    for obj in client.list_objects(bucket, prefix=prefix, recursive=False):
        name = obj.object_name or ""
        for part in name.split("/"):
            if part.startswith("dt="):
                dates.add(part[3:])

    result = sorted(dates)
    _log.info("Found %d partition(s) for %s/%s", len(result), layer, source)
    return result