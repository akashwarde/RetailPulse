import pyspark.sql.functions as F
"""Unit tests for the RetailPulse configuration-driven validation engine."""

from datetime import datetime, timedelta
import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from retailpulse.validation import (
    RuleType,
    ValidationRule,
    get_source_rules,
    validate,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Session-scoped SparkSession fixture for testing."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("RetailPulseValidationTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


class TestRuleEngine:
    """Test suite for the validation engine and all supported rule types."""

    def test_validate_all_valid(self, spark: SparkSession) -> None:
        """All valid records should pass into good_df with zero records in bad_df."""
        data = [
            (101, "Aarav", 25, "active"),
            (102, "Diya", 30, "active"),
            (103, "Isha", 28, "inactive"),
        ]
        df = spark.createDataFrame(data, ["id", "name", "age", "status"])

        rules = [
            ValidationRule.not_null("id"),
            ValidationRule.not_null("name"),
            ValidationRule.range("age", min_val=18, max_val=100),
            ValidationRule.allowed_values("status", ["active", "inactive"]),
            ValidationRule.unique("id"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 3
        assert bad_df.count() == 0
        assert good_df.columns == df.columns
        assert set(bad_df.columns) == set(df.columns + ["rule_failed", "reason"])

    def test_not_null_rule(self, spark: SparkSession) -> None:
        """Records with null values in required columns must be routed to bad_df."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        data = [
            (1, "Valid Name"),
            (2, None),
            (None, "Another Valid"),
        ]
        df = spark.createDataFrame(data, schema)

        rules = [
            ValidationRule.not_null("id"),
            ValidationRule.not_null("name"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 2

        bad_rows = {row["id"]: row["rule_failed"] for row in bad_df.collect()}
        assert "not_null:name" in bad_rows[2]
        assert "not_null:id" in bad_rows[None]

    def test_range_rule_numeric(self, spark: SparkSession) -> None:
        """Numeric values below min or above max should fail."""
        data = [
            (1, 10, 49.99),
            (2, -5, 20.00),   # invalid qty
            (3, 0, -1.50),    # invalid qty and price
            (4, 5, 100.00),
        ]
        df = spark.createDataFrame(data, ["item_id", "qty", "price"])

        rules = [
            ValidationRule.range("qty", min_val=1, inclusive_min=True, name="qty_gt_0"),
            ValidationRule.range("price", min_val=0.0, inclusive_min=True, name="price_gte_0"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 2
        assert bad_df.count() == 2

        bad_rows = {row["item_id"]: row["rule_failed"] for row in bad_df.collect()}
        assert bad_rows[2] == "qty_gt_0"
        assert "qty_gt_0" in bad_rows[3]
        assert "price_gte_0" in bad_rows[3]

    def test_range_rule_future_date(self, spark: SparkSession) -> None:
        """Future timestamps must be detected and rejected."""
        past_time = datetime(2025, 1, 15, 10, 30, 0)
        future_time = datetime.now() + timedelta(days=365)

        data = [
            (1, past_time),
            (2, future_time),
        ]
        df = spark.createDataFrame(data, ["id", "event_time"])

        rules = [
            ValidationRule.range("event_time", max_val="now", name="event_time_not_future"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 1

        bad_row = bad_df.collect()[0]
        assert bad_row["id"] == 2
        assert bad_row["rule_failed"] == "event_time_not_future"
        assert "is in the future" in bad_row["reason"]

    def test_regex_format_rule(self, spark: SparkSession) -> None:
        """Valid emails must pass, invalid or malformed emails must fail."""
        data = [
            (1, "aarav@example.com"),
            (2, "bad-email-at-domain.com"),
            (3, "missing_at_symbol.com"),
            (4, "diya.patel@retail.co.in"),
        ]
        df = spark.createDataFrame(data, ["customer_id", "email"])

        rules = [
            ValidationRule.regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$", name="valid_email"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 2
        assert bad_df.count() == 2

        bad_ids = {row["customer_id"] for row in bad_df.collect()}
        assert bad_ids == {2, 3}

    def test_allowed_values_rule(self, spark: SparkSession) -> None:
        """Categorical columns with values outside allowed set must fail."""
        data = [
            (1, "UPI"),
            (2, "Card"),
            (3, "Bitcoin"),   # disallowed
            (4, "Cash"),
        ]
        df = spark.createDataFrame(data, ["order_id", "payment_method"])

        rules = [
            ValidationRule.allowed_values(
                "payment_method",
                ["UPI", "Card", "Cash", "Net Banking"],
                name="allowed_payment_methods",
            ),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 3
        assert bad_df.count() == 1
        bad_row = bad_df.collect()[0]
        assert bad_row["order_id"] == 3
        assert bad_row["rule_failed"] == "allowed_payment_methods"

    def test_unique_rule_single_column(self, spark: SparkSession) -> None:
        """Single-column uniqueness retains first occurrence and flags subsequent duplicates."""
        data = [
            (10, "Alpha"),
            (20, "Beta"),
            (10, "Alpha Duplicate"),
        ]
        df = spark.createDataFrame(data, ["id", "name"])

        rules = [ValidationRule.unique("id", name="unique_id")]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 2
        assert bad_df.count() == 1

        bad_row = bad_df.collect()[0]
        assert bad_row["name"] == "Alpha Duplicate"
        assert bad_row["rule_failed"] == "unique_id"
        assert "Duplicate record for key" in bad_row["reason"]

    def test_unique_rule_composite_key(self, spark: SparkSession) -> None:
        """Composite uniqueness on (order_id, item_id) retains first line, rejects duplicate."""
        data = [
            (1001, 1, 10),
            (1001, 2, 20),
            (1001, 1, 15),  # duplicate line
            (1002, 1, 30),
        ]
        df = spark.createDataFrame(data, ["order_id", "item_id", "qty"])

        rules = [
            ValidationRule.unique(["order_id", "item_id"], name="unique_order_line"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 3
        assert bad_df.count() == 1
        bad_row = bad_df.collect()[0]
        assert bad_row["order_id"] == 1001
        assert bad_row["item_id"] == 1
        assert bad_row["qty"] == 15
        assert bad_row["rule_failed"] == "unique_order_line"

    def test_referential_rule_with_ref_df(self, spark: SparkSession) -> None:
        """Referential check with reference DataFrame must validate foreign keys."""
        master_products = spark.createDataFrame(
            [(1, "Mouse"), (2, "Keyboard"), (3, "Monitor")],
            ["product_id", "prod_name"],
        )

        orders_data = [
            (101, 1),
            (102, 2),
            (103, 999),  # product does not exist
        ]
        orders_df = spark.createDataFrame(orders_data, ["order_id", "product_id"])

        rules = [
            ValidationRule.referential(
                column="product_id",
                ref_df=master_products,
                ref_column="product_id",
                name="fk_product_id",
            )
        ]

        good_df, bad_df = validate(orders_df, rules)

        assert good_df.count() == 2
        assert bad_df.count() == 1
        bad_row = bad_df.collect()[0]
        assert bad_row["order_id"] == 103
        assert bad_row["rule_failed"] == "fk_product_id"
        assert "foreign key does not exist" in bad_row["reason"]

    def test_referential_rule_with_values(self, spark: SparkSession) -> None:
        """Referential check with in-memory value list must validate foreign keys."""
        valid_store_ids = [1, 2, 3, 4, 5]
        data = [
            (101, 1),
            (102, 99),  # invalid store
        ]
        df = spark.createDataFrame(data, ["order_id", "store_id"])

        rules = [
            ValidationRule.referential(
                column="store_id",
                ref_values=valid_store_ids,
                name="fk_store_id",
            )
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 1
        assert bad_df.collect()[0]["store_id"] == 99

    def test_multiple_failing_rules_on_single_row(self, spark: SparkSession) -> None:
        """A row failing several rules must record all of them in rule_failed and reason."""
        schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("qty", IntegerType(), True),
            StructField("email", StringType(), True),
        ])
        data = [
            (100, 5, "valid@example.com"),
            (None, -10, "invalid-email"),
        ]
        df = spark.createDataFrame(data, schema)

        rules = [
            ValidationRule.not_null("customer_id", name="not_null:customer_id"),
            ValidationRule.range("qty", min_val=1, name="range:qty"),
            ValidationRule.regex("email", r"^[\w\.-]+@[\w\.-]+\.\w+$", name="regex:email"),
        ]

        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 1

        bad_row = bad_df.collect()[0]
        failed_list = [r.strip() for r in bad_row["rule_failed"].split(",")]
        assert "not_null:customer_id" in failed_list
        assert "range:qty" in failed_list
        assert "regex:email" in failed_list

        reasons = bad_row["reason"]
        assert "customer_id" in reasons
        assert "qty" in reasons
        assert "email" in reasons

    def test_dict_rule_support(self, spark: SparkSession) -> None:
        """Rules defined as dictionaries must be parsed and executed cleanly."""
        data = [
            (1, 100),
            (None, 50),
            (2, -10),
        ]
        df = spark.createDataFrame(data, ["id", "amount"])

        dict_rules = [
            {"type": "not_null", "column": "id", "name": "check_id_null"},
            {"type": "range", "column": "amount", "min_val": 0, "name": "check_amount_positive"},
        ]

        good_df, bad_df = validate(df, dict_rules)

        assert good_df.count() == 1
        assert bad_df.count() == 2

    def test_source_rules_sales(self, spark: SparkSession) -> None:
        """Predefined SALES_RULES must catch injected defects."""
        now = datetime.now()
        data = [
            (1, 10, 2, 99.0, now - timedelta(days=1), 1001),   # valid
            (2, None, 1, 50.0, now - timedelta(days=1), 1002), # null customer_id
            (3, 11, -1, 20.0, now - timedelta(days=1), 1003),  # qty <= 0
            (4, 12, 1, 30.0, now + timedelta(days=5), 1004),   # future date
            (1, 10, 2, 99.0, now - timedelta(days=1), 1001),   # duplicate order line
        ]
        df = spark.createDataFrame(
            data,
            ["order_id", "customer_id", "quantity", "unit_price", "order_timestamp", "order_item_id"],
        )

        df = df.withColumn("product_id", F.lit(1)).withColumn("store_id", F.lit(1))
        rules = get_source_rules("sales")
        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 4

    def test_source_rules_inventory(self, spark: SparkSession) -> None:
        """Predefined INVENTORY_RULES must catch stock < 0 and missing product_id."""
        schema = StructType([
            StructField("product_id", IntegerType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("stock_qty", IntegerType(), True),
            StructField("snapshot_date", StringType(), True),
        ])
        data = [
            (1, 10, 50, "2025-01-01"),
            (None, 10, 50, "2025-01-01"),  # null product_id
            (2, 10, -5, "2025-01-01"),     # stock < 0
        ]
        df = spark.createDataFrame(data, schema)

        rules = get_source_rules("inventory")
        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 2

    def test_source_rules_customers(self, spark: SparkSession) -> None:
        """Predefined CUSTOMERS_RULES must catch bad email, missing city, duplicate id."""
        data = [
            (1, "Aarav", "Mumbai", "aarav@example.com"),
            (2, "Diya", None, "diya@example.com"),       # missing city
            (3, "Isha", "Pune", "bad-email-format"),     # bad email
            (1, "Aarav Dup", "Mumbai", "aarav2@example.com"), # duplicate customer_id
        ]
        df = spark.createDataFrame(data, ["customer_id", "first_name", "city", "email"])

        df = df.withColumn("signup_date", F.lit("2024-01-01"))
        rules = get_source_rules("customers")
        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 1
        assert bad_df.count() == 3

    def test_empty_dataframe(self, spark: SparkSession) -> None:
        """Validation on an empty DataFrame returns empty DataFrames with correct schemas."""
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
        ])
        df = spark.createDataFrame([], schema)

        rules = [ValidationRule.not_null("id")]
        good_df, bad_df = validate(df, rules)

        assert good_df.count() == 0
        assert bad_df.count() == 0
        assert good_df.columns == ["id", "name"]
        assert bad_df.columns == ["id", "name", "rule_failed", "reason"]

    def test_empty_rules_list(self, spark: SparkSession) -> None:
        """Validation with zero rules accepts all rows without error."""
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        good_df, bad_df = validate(df, [])

        assert good_df.count() == 2
        assert bad_df.count() == 0

    def test_missing_column_raises_error(self, spark: SparkSession) -> None:
        """Validation with a non-existent column must raise ValueError."""
        df = spark.createDataFrame([(1, "a")], ["id", "val"])
        rules = [ValidationRule.not_null("non_existent_column")]

        with pytest.raises(ValueError, match="non_existent_column"):
            validate(df, rules)
