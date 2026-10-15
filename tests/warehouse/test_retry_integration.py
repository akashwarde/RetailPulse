"""Live PostgreSQL regression checks; never connect to production by default."""
from unittest.mock import patch

from retailpulse.warehouse.load_dimensions import load_dim_product, load_dim_customer, load_dim_store
from retailpulse.warehouse.load_facts import load_fact_sales, load_fact_inventory


def masters(conn):
    load_dim_product(conn, data=[dict(product_id=1, name="Widget", category="Home", price=10)])
    load_dim_customer(conn, data=[dict(customer_id=1, first_name="Demo", city="Pune")])
    load_dim_store(conn, data=[dict(store_id=1, city="Pune")])


def sale(**overrides):
    return dict(dict(order_id=1, order_item_id=1, product_id=1, customer_id=1,
                     store_id=1, quantity=1, unit_price=10, order_timestamp="2025-01-15T10:00:00"), **overrides)


def count(conn, table):
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]


def test_online_batches_replay_and_physical_isolation(pg):
    masters(pg)
    load_fact_sales(pg, data=[sale()], run_date="2025-01-15")
    a = sale(event_id="a", channel="online", store_id=None)
    b = sale(event_id="b", channel="online", store_id=None, order_id=2)
    load_fact_sales(pg, data=[a], run_date="2025-01-15", channel="online")
    load_fact_sales(pg, data=[b], run_date="2025-01-15", channel="online")
    load_fact_sales(pg, data=[a], run_date="2025-01-15", channel="online")
    assert count(pg, "dw.fact_sales") == 3
    load_fact_sales(pg, data=[], run_date="2025-01-15", channel="in_store")
    assert count(pg, "dw.fact_sales") == 2


def test_missing_dimension_is_persisted_once_and_corrected_batch_clears_it(pg):
    masters(pg)
    for _ in range(2):
        result = load_fact_sales(pg, data=[sale(product_id=999)], run_date="2025-01-15")
        assert result.quarantined == 1
    assert count(pg, "dq.quarantine") == 1
    with pg.cursor() as cur:
        cur.execute("SELECT reason FROM dq.quarantine")
        assert "missing dimension" in cur.fetchone()[0]
    load_fact_sales(pg, data=[sale()], run_date="2025-01-15")
    assert count(pg, "dq.quarantine") == 0
    assert count(pg, "dw.fact_sales") == 1


def test_inventory_snapshot_removes_omitted_rows_and_empty_correction(pg):
    masters(pg)
    row = dict(product_id=1, store_id=1, stock_qty=5, snapshot_date="2025-01-15")
    for _ in range(2):
        load_fact_inventory(pg, data=[row], run_date="2025-01-15")
    assert count(pg, "dw.fact_inventory") == 1
    load_fact_inventory(pg, data=[], run_date="2025-01-15")
    assert count(pg, "dw.fact_inventory") == 0


def test_failed_fact_write_rolls_back_snapshot_deletion(pg):
    masters(pg)
    load_fact_sales(pg, data=[sale()], run_date="2025-01-15")
    with patch("retailpulse.warehouse.load_facts.execute_batch", side_effect=RuntimeError("write failed")):
        import pytest
        with pytest.raises(RuntimeError, match="write failed"):
            load_fact_sales(pg, data=[sale(quantity=2)], run_date="2025-01-15")
    assert count(pg, "dw.fact_sales") == 1
    with pg.cursor() as cur:
        cur.execute("SELECT quantity FROM dw.fact_sales")
        assert cur.fetchone()[0] == 1
