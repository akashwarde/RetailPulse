"""Regression checks for cast failures, reference checks and latest survivors."""
import pyarrow as pa

from retailpulse.transform.preparation import prepare, from_bronze
from retailpulse.validation import validate, SALES_RULES, INVENTORY_RULES, CUSTOMERS_RULES, ValidationRule


def sale(**changes):
    return dict(dict(order_id="1", order_item_id="1", customer_id="1", store_id="1", product_id="1",
                     quantity="1", unit_price="10", order_timestamp="2025-01-15T10:00:00"), **changes)


def test_malformed_values_are_rejected_instead_of_becoming_silver_nulls(spark):
    rows = [sale(order_id=str(i + 1), **changes) for i, changes in enumerate([
        {}, {"quantity": "1.5"}, {"unit_price": "bad"}, {"unit_price": "NaN"},
        {"unit_price": "0"}, {"order_timestamp": "bad-date"}, {"product_id": "999"},
    ])]
    df = spark.createDataFrame(rows)
    rules = [*SALES_RULES, ValidationRule.referential("product_id", ref_values=["1"])]
    good, bad = validate(df, rules)
    assert good.count() == 1 and bad.count() == 6


def test_deduplication_keeps_latest_valid_record(spark):
    rows = [sale(quantity="1"), sale(quantity="2", order_timestamp="2025-01-15T11:00:00"),
            sale(customer_id=None, order_timestamp="2025-01-15T12:00:00")]
    good, bad = validate(spark.createDataFrame(rows), SALES_RULES)
    assert good.collect()[0]["quantity"] == "2"
    assert good.count() == 1 and bad.count() == 2


def test_inventory_normalizes_dates_before_duplicate_selection(spark):
    rows = [dict(product_id="1", store_id="1", stock_qty="4", snapshot_date="15/01/2025", _ingested_at="2025-01-15T10:00:00"),
            dict(product_id="1", store_id="1", stock_qty="7", snapshot_date="2025-01-15", _ingested_at="2025-01-15T11:00:00")]
    good, bad = validate(prepare(spark.createDataFrame(rows), "inventory"), INVENTORY_RULES)
    assert good.count() == 1 and bad.count() == 1
    assert good.collect()[0]["stock_qty"] == "7"


def test_customer_date_cast_failures_and_blank_city_are_rejected(spark):
    rows = [dict(customer_id="1", first_name="Demo", email=" DEMO@EXAMPLE.COM ", city="Pune", signup_date="2024-01-01"),
            dict(customer_id="2", first_name="Demo", email="demo@example.com", city="Pune", signup_date="bad"),
            dict(customer_id="3", first_name="Demo", email="demo@example.com", city="   ", signup_date="2024-01-01")]
    good, bad = validate(prepare(spark.createDataFrame(rows), "customers"), CUSTOMERS_RULES)
    assert good.count() == 1 and bad.count() == 2


def test_empty_bronze_partition_has_an_explicit_schema(spark):
    table = pa.table({key: pa.array([], type=pa.string()) for key in sale()})
    good, bad = validate(from_bronze(spark, table), SALES_RULES)
    assert good.count() == 0 and bad.count() == 0
