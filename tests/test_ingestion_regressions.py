"""Regression checks for missing batch stages and mismatched source contracts."""
import csv
import json
from unittest.mock import MagicMock

import pytest

from retailpulse.common.config import AppConfig, PostgresConfig, MinIOConfig
from retailpulse.common.lake import read_parquet, build_lake_path
from retailpulse.generators.demo import generate, CUSTOMER_COUNT, PRODUCT_COUNT
from retailpulse.ingestion.batch import extract_inventory, extract_customers, extract_masters, extract_sales
from retailpulse.ingestion.stream.order_generator import OrderGenerator
from tests.fakes import Lake


@pytest.fixture
def cfg():
    return AppConfig(postgres=PostgresConfig(password="test"), minio=MinIOConfig(secret_key="test"))


def test_generator_counts_defects_and_reproducibility(tmp_path):
    counts = generate(days=2, orders=1000, defect_rate=0.1, end_date="2025-01-15", output_dir=tmp_path, load_db=False)
    assert counts["products"] == 50 and counts["stores"] == 5 and counts["customers"] == 1000
    before = {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    generate(days=2, orders=1000, defect_rate=0.1, end_date="2025-01-15", output_dir=tmp_path, load_db=False)
    assert before == {str(p.relative_to(tmp_path)): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    rows = list(csv.DictReader((tmp_path / "sales.csv").open()))
    assert len(rows) == 1000
    assert any(not r["customer_id"] for r in rows)
    assert any(int(r["quantity"]) < 0 for r in rows)
    assert len(list((tmp_path / "inventory").glob("*.csv"))) == 2
    assert len(json.loads((tmp_path / "customers.json").read_text())) == 1000


def test_file_extractors_roundtrip_retry_and_changed_file(tmp_path, cfg):
    generate(days=1, orders=30, defect_rate=0, end_date="2025-01-15", output_dir=tmp_path, load_db=False)
    lake = Lake()
    kwargs = dict(run_date="2025-01-15", data_dir=tmp_path, config=cfg, client=lake)
    result = extract_inventory(**kwargs)
    assert result["rows"] == 250
    writes = lake.writes
    assert extract_inventory(**kwargs)["skipped"]
    assert lake.writes == writes
    path = tmp_path / "inventory/inventory_2025-01-15.csv"
    path.write_text(path.read_text().replace("product_id", "Product ID"))
    assert not extract_inventory(**kwargs).get("skipped", False)
    extract_customers(**kwargs)
    extract_masters(**kwargs)
    table = read_parquet(lake, cfg.minio.bucket, build_lake_path("bronze", "customers", "2025-01-15"))
    assert table.num_rows == 1000
    assert set(["_source_file", "_batch_id", "_ingested_at"]) <= set(table.column_names)
    assert read_parquet(lake, cfg.minio.bucket, build_lake_path("silver", "products", "2025-01-15")).num_rows == 50
    path.write_text("unknown,header\n1,2\n")
    with pytest.raises(ValueError, match="headers"):
        extract_inventory(**kwargs)


def test_missing_inventory_is_explicit(tmp_path, cfg):
    with pytest.raises(FileNotFoundError):
        extract_inventory("2025-01-15", tmp_path, cfg, Lake())


def test_sales_query_is_date_scoped_and_preserves_invalid_rows(cfg):
    conn, lake = MagicMock(), Lake()
    conn.cursor.return_value.__enter__.return_value.fetchall.return_value = [
        dict(order_id=1, order_item_id=1, customer_id=None, store_id=1, product_id=1,
             quantity=-1, unit_price=10, order_timestamp="bad", order_status="COMPLETED", payment_method="CARD")]
    result = extract_sales("2025-01-15", cfg, lake, conn)
    assert result["rows"] == 1
    query, params = conn.cursor.return_value.__enter__.return_value.execute.call_args.args
    assert "source_date = %s" in query and params == ("2025-01-15",)
    table = read_parquet(lake, cfg.minio.bucket, result["path"])
    assert table.to_pylist()[0]["customer_id"] is None
    assert table.to_pylist()[0]["quantity"] == "-1"


def test_producer_uses_shared_master_ranges():
    generator = OrderGenerator(seed=42)
    for _ in range(1000):
        event = generator.generate_valid_event().data
        assert 1 <= event["customer_id"] <= CUSTOMER_COUNT
        assert 1 <= event["product_id"] <= PRODUCT_COUNT
