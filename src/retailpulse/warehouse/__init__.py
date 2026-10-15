"""RetailPulse warehouse package."""

from retailpulse.warehouse.load_dimensions import (
    DimensionLoadResult,
    ensure_date_key,
    load_dim_customer,
    load_dim_date,
    load_dim_product,
    load_dim_store,
    load_dimensions,
    main as load_dimensions_main,
)
from retailpulse.warehouse.load_facts import (
    FactLoadResult,
    fetch_dimension_lookups,
    load_fact_inventory,
    load_fact_sales,
    load_facts,
    main as load_facts_main,
)
from retailpulse.warehouse.marts import (
    get_channel_performance,
    get_daily_revenue,
    get_dq_summary,
    get_low_stock_alerts,
    get_top_products,
    refresh_marts,
)

__all__ = [
    # Dimension loading
    "DimensionLoadResult",
    "ensure_date_key",
    "load_dim_date",
    "load_dim_product",
    "load_dim_customer",
    "load_dim_store",
    "load_dimensions",
    "load_dimensions_main",
    # Fact loading
    "FactLoadResult",
    "fetch_dimension_lookups",
    "load_fact_sales",
    "load_fact_inventory",
    "load_facts",
    "load_facts_main",
    # Marts & Analytical Reporting
    "refresh_marts",
    "get_daily_revenue",
    "get_top_products",
    "get_low_stock_alerts",
    "get_dq_summary",
    "get_channel_performance",
]
