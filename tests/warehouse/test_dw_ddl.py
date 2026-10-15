"""Unit tests for the Data Warehouse star schema DDL definitions."""

from pathlib import Path
import pytest


@pytest.fixture(scope="module")
def ddl_content() -> str:
    """Load the DDL content from sql/ddl/03_dw_schema.sql."""
    ddl_path = Path("sql/ddl/03_dw_schema.sql")
    assert ddl_path.exists(), f"Expected DDL file at {ddl_path.resolve()}"
    return ddl_path.read_text(encoding="utf-8")


class TestWarehouseStarSchemaDDL:
    """Verifies that sql/ddl/03_dw_schema.sql fulfills all Phase 2 Task 2.5 requirements."""

    def test_schema_creation_is_idempotent(self, ddl_content: str) -> None:
        """Check that dw schema is created idempotently."""
        assert "CREATE SCHEMA IF NOT EXISTS dw;" in ddl_content

    def test_dimension_tables_exist_with_surrogate_and_business_keys(self, ddl_content: str) -> None:
        """Verify all 4 dimension tables are defined with required keys."""
        # dim_date
        assert "CREATE TABLE IF NOT EXISTS dw.dim_date" in ddl_content
        assert "date_key" in ddl_content
        assert "full_date" in ddl_content
        assert "is_weekend" in ddl_content

        # dim_product
        assert "CREATE TABLE IF NOT EXISTS dw.dim_product" in ddl_content
        assert "product_key" in ddl_content
        assert "product_id" in ddl_content
        assert "name" in ddl_content
        assert "category" in ddl_content
        assert "price" in ddl_content

        # dim_customer
        assert "CREATE TABLE IF NOT EXISTS dw.dim_customer" in ddl_content
        assert "customer_key" in ddl_content
        assert "customer_id" in ddl_content
        assert "city" in ddl_content
        assert "segment" in ddl_content

        # dim_store
        assert "CREATE TABLE IF NOT EXISTS dw.dim_store" in ddl_content
        assert "store_key" in ddl_content
        assert "store_id" in ddl_content
        assert "store_type" in ddl_content or "type" in ddl_content

    def test_date_dimension_generator(self, ddl_content: str) -> None:
        """Verify date dimension generates 2 years of dates idempotently."""
        assert "generate_series" in ddl_content
        assert "2024-01-01" in ddl_content
        assert "2025-12-31" in ddl_content
        assert "ON CONFLICT (date_key) DO NOTHING" in ddl_content

    def test_fact_tables_and_foreign_keys(self, ddl_content: str) -> None:
        """Verify fact_sales and fact_inventory schemas and relationships."""
        # fact_sales
        assert "CREATE TABLE IF NOT EXISTS dw.fact_sales" in ddl_content
        assert "sales_key" in ddl_content
        assert "order_id" in ddl_content
        assert "REFERENCES dw.dim_date(date_key)" in ddl_content
        assert "REFERENCES dw.dim_product(product_key)" in ddl_content
        assert "REFERENCES dw.dim_customer(customer_key)" in ddl_content
        assert "REFERENCES dw.dim_store(store_key)" in ddl_content
        assert "quantity" in ddl_content
        assert "revenue" in ddl_content
        assert "channel" in ddl_content
        assert "in_store" in ddl_content and "online" in ddl_content

        # fact_inventory
        assert "CREATE TABLE IF NOT EXISTS dw.fact_inventory" in ddl_content
        assert "inventory_key" in ddl_content
        assert "stock_qty" in ddl_content
        assert "uq_fact_inventory" in ddl_content or "UNIQUE (date_key, product_key, store_key)" in ddl_content

    def test_foreign_key_indexes(self, ddl_content: str) -> None:
        """Verify indexes on fact foreign keys are present."""
        assert "idx_fact_sales_date" in ddl_content
        assert "idx_fact_sales_product" in ddl_content
        assert "idx_fact_sales_customer" in ddl_content
        assert "idx_fact_sales_store" in ddl_content
        assert "idx_fact_sales_channel" in ddl_content
        assert "idx_fact_inventory_date" in ddl_content
        assert "idx_fact_inventory_product" in ddl_content
        assert "idx_fact_inventory_store" in ddl_content

    def test_analytical_mart_views(self, ddl_content: str) -> None:
        """Verify all 4 required mart views are defined."""
        assert "CREATE OR REPLACE VIEW dw.mart_daily_revenue" in ddl_content
        assert "CREATE OR REPLACE VIEW dw.mart_top_products" in ddl_content
        assert "CREATE OR REPLACE VIEW dw.mart_low_stock" in ddl_content
        assert "CREATE OR REPLACE VIEW dw.mart_dq_summary" in ddl_content

        # Mart views source references
        assert "FROM dw.fact_sales" in ddl_content
        assert "FROM dw.fact_inventory" in ddl_content
        assert "FROM dq.summary" in ddl_content
