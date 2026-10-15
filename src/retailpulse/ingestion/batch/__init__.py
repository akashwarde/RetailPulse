"""Batch ingestion entrypoints for Airflow tasks."""
from retailpulse.ingestion.batch.extractors import (
    extract_customers, extract_inventory, extract_masters, extract_sales,
)

__all__ = ["extract_customers", "extract_inventory", "extract_masters", "extract_sales"]
