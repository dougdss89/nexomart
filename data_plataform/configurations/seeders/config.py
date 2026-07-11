"""
Seeder configuration — dynamic volume and chaos-rate ranges per schema/table.

Values are randomly sampled at runtime so each DAG execution produces realistic,
unpredictable load: normal days vs. peak events (Black Friday, Christmas, etc.).
"""

from dataclasses import dataclass
from typing import Dict, Tuple


@dataclass
class SchemaRanges:
    min_count: int
    max_count: int
    min_chaos: float
    max_chaos: float
    absolute_max: int | None = None
    description: str = ""


# ── Per-schema ranges (used when no table-level override exists) ──────────────

SCHEMA_RANGES: Dict[str, SchemaRanges] = {
    "sales": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Normal days (100k) to peak demand (Black Friday: 50M)",
    ),
    "purchasing": SchemaRanges(
        min_count=50_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Combined header + detail volume with high variance",
    ),
    "warehouse": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Stock movements with peaks during high-demand periods",
    ),
    "humanresources": SchemaRanges(
        min_count=50,
        max_count=100,
        min_chaos=0.0,
        max_chaos=0.0,
        description="Small, stable HR dimension — no chaos (critical data)",
    ),
    "product": SchemaRanges(
        min_count=500,
        max_count=1_500,
        min_chaos=0.02,
        max_chaos=0.05,
        absolute_max=100_000,
        description="Product catalog with 100k absolute ceiling",
    ),
    "person": SchemaRanges(
        min_count=10_000,
        max_count=5_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Customers proportional to sales volume (1:N)",
    ),
    "marketing": SchemaRanges(
        min_count=1_000,
        max_count=500_000,
        min_chaos=0.01,
        max_chaos=0.08,
        description="Campaigns and ads derived from SKUs and campaign periods",
    ),
}

# ── Per-table overrides (take priority over schema-level ranges) ──────────────

TABLE_RANGES: Dict[str, SchemaRanges] = {
    "sales.salesorder": SchemaRanges(
        min_count=100_000, max_count=50_000_000, min_chaos=0.01, max_chaos=0.10,
        description="Primary sales fact table",
    ),
    "sales.sales_return_order": SchemaRanges(
        min_count=5_000, max_count=500_000, min_chaos=0.02, max_chaos=0.10,
        description="Returns (~2-5% of sales volume)",
    ),
    "warehouse.transaction_history": SchemaRanges(
        min_count=200_000, max_count=50_000_000, min_chaos=0.02, max_chaos=0.10,
        description="Stock transaction history (multiple moves per order)",
    ),
    "warehouse.product_inventory": SchemaRanges(
        min_count=10_000, max_count=100_000, min_chaos=0.03, max_chaos=0.08,
        description="Current inventory (one row per product per location)",
    ),
    "person.customer": SchemaRanges(
        min_count=50_000, max_count=5_000_000, min_chaos=0.03, max_chaos=0.10,
        description="Primary customer dimension",
    ),
    "person.address": SchemaRanges(
        min_count=50_000, max_count=10_000_000, min_chaos=0.05, max_chaos=0.10,
        description="Addresses (1:N with customers)",
    ),
    "product.product": SchemaRanges(
        min_count=500, max_count=1_500, min_chaos=0.02, max_chaos=0.05,
        absolute_max=100_000,
        description="Product catalog",
    ),
    "product.product_category": SchemaRanges(
        min_count=20, max_count=50, min_chaos=0.0, max_chaos=0.0,
        description="Stable product categories",
    ),
    "humanresources.employee": SchemaRanges(
        min_count=50, max_count=100, min_chaos=0.0, max_chaos=0.0,
        description="Employees — small and critical",
    ),
    "humanresources.department": SchemaRanges(
        min_count=10, max_count=20, min_chaos=0.0, max_chaos=0.0,
        description="Departments lookup table",
    ),
    "purchasing.purchaseorderheader": SchemaRanges(
        min_count=50_000, max_count=10_000_000, min_chaos=0.01, max_chaos=0.08,
        description="Purchase order headers",
    ),
    "purchasing.purchaseorderdetail": SchemaRanges(
        min_count=100_000, max_count=50_000_000, min_chaos=0.02, max_chaos=0.10,
        description="Purchase order line items (multiple per header)",
    ),
    "marketing.marketing_campaign": SchemaRanges(
        min_count=100, max_count=5_000, min_chaos=0.02, max_chaos=0.05,
        description="Marketing campaigns",
    ),
    "marketing.marketing_daily_performance": SchemaRanges(
        min_count=10_000, max_count=500_000, min_chaos=0.01, max_chaos=0.05,
        description="Daily ad performance time-series",
    ),
}

# ── Recommended chaos types per schema ───────────────────────────────────────

RECOMMENDED_CHAOS_TYPES: Dict[str, list[str]] = {
    "sales": [
        "duplicate", "dirty_cpf", "dirty_email",
        "fraud_fake_cpf", "fraud_cloned_card", "fraud_high_value_suspicious",
        "incomplete_payment", "incomplete_shipping", "incomplete_delivery",
        "negative_values",
    ],
    "purchasing": [
        "duplicate", "fraud_duplicate_order", "fraud_phantom_vendor",
        "fraud_overpriced", "negative_cost", "missing_vendor_ref", "incomplete_delivery",
    ],
    "warehouse": [
        "warehouse_lost_transfer", "warehouse_quantity_mismatch",
        "warehouse_negative_stock", "warehouse_missing_location",
        "warehouse_quality_fail", "warehouse_cold_chain_break",
        "warehouse_same_location_transfer", "warehouse_missing_traceability",
        "missing_order_ref",
    ],
    "person": [
        "duplicate", "dirty_cpf", "dirty_phone", "dirty_email",
        "dirty_cep", "dirty_name", "fraud_fake_address",
    ],
    "product": ["duplicate", "outlier_value"],
    "marketing": ["outlier_value", "outlier_quantity", "negative_values"],
    "humanresources": [],
}


# ── Public API ────────────────────────────────────────────────────────────────

def get_ranges(schema: str = None, table: str = None) -> SchemaRanges:
    """Return ranges for a table (highest priority) or schema, with a safe fallback."""
    if table and table in TABLE_RANGES:
        return TABLE_RANGES[table]
    if schema and schema in SCHEMA_RANGES:
        return SCHEMA_RANGES[schema]
    return SchemaRanges(
        min_count=1_000, max_count=100_000, min_chaos=0.02, max_chaos=0.08,
        description="Default fallback for unmapped schemas",
    )


def get_default_ranges(schema: str) -> Tuple[int, int, float, float]:
    """Return (min_count, max_count, min_chaos, max_chaos) — useful for argparse defaults."""
    r = get_ranges(schema=schema)
    return r.min_count, r.max_count, r.min_chaos, r.max_chaos


def validate_absolute_max(schema: str, table: str = None) -> int | None:
    """Return the absolute row ceiling for a schema/table, or None if uncapped."""
    return get_ranges(schema=schema, table=table).absolute_max


def get_recommended_chaos_types(schema: str) -> list[str]:
    """Return the list of recommended chaos injection types for a schema."""
    return RECOMMENDED_CHAOS_TYPES.get(schema, [])
