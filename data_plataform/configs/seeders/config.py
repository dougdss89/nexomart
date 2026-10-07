"""
Seeder configuration — dynamic volume and chaos-rate ranges per schema/table.

Values are randomly sampled at runtime so each DAG execution produces realistic,
unpredictable load: normal days vs. peak events (Black Friday, Christmas, etc.).

Ranges below are calibrated for `nexomart_seeder_dag` running every 10-15 minutes
(~100 runs/day), not once a day. They used to be sized for "a full day's worth of
activity in one run" (up to 1M rows per table) — that was fine for a daily cron, but a
10-15 min cadence turns each run into a micro-batch: a run's row count should be roughly
(the old daily range) / (runs per day), not the whole day's range every time. Rescaled
2026-08-17 with a /100 factor (matches ~15min cadence, close enough for ~10min too) —
see docs/SEEDER_MICROBATCHING.md for the measured task durations that motivated this and
the reasoning behind the divisor. If the schedule interval changes meaningfully, rescale
these again (divisor = target runs/day) rather than leaving stale day-sized ranges in a
tighter loop.

This alone doesn't fully fix runaway growth for the fan-out seeders (customer_email,
purchase_detail, purchase_audit) — those derive their count from *pending* parent rows,
capped by the parent table's own max_count below, not from an independent range here. See
docs/SEEDER_MICROBATCHING.md for how those were fixed (anti-join + LIMIT instead of a full
table diff in Python).

Updated 2026-08-21 (user request, not re-measured): every range that was maxed out at 10,000
(sales, purchasing, warehouse, person schemas + their heaviest tables) raised to 1,000-50,000 —
5x the previous ceiling. The 2026-08-17 measurements in docs/SEEDER_MICROBATCHING.md (18m36s
for a full DAG run at the old 10k ceilings) do NOT cover this range — a max-volume run under the
new ceilings hasn't been timed and could run considerably longer, especially for
purchaseorderheader (which also bounds the purchaseorderdetail/purchaseorderaudit fan-out) and
person.customer (which bounds customer_email). Re-measure before trusting this against a tight
DAG cadence. Chaos rates also raised: min 5% everywhere (was 0-5% depending on table, including
humanresources/product_category/employee/department which were deliberately 0% as "critical, no
chaos" data — that guarantee no longer holds), max 8% for any table whose max was previously
<=5%, left alone elsewhere.

Updated 2026-08-21 (user request, for a manual test run — not re-measured): sales (schema +
sales.salesorder) raised to 20,000-200,000 (was 1,000-50,000); person (schema + person.customer)
min raised to 10,000, max unchanged at 50,000 (was 1,000-50,000). purchasing/warehouse and
person.address were left as-is. Chaos rates unchanged. Given the 18m36s full-DAG measurement at
10k ceilings (docs/SEEDER_MICROBATCHING.md), a max-volume sales run here (200k rows, 4x the
previous 50k ceiling) is untimed and could run long against the 10-15min DAG cadence — re-measure
before trusting this in production.
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
        min_count=20_000,
        max_count=200_000,
        min_chaos=0.05,
        max_chaos=0.10,
        description="Normal runs (20k) to peak demand (200k) per ~10-15min DAG run — bumped 2026-08-21 for testing, was 1k-50k",
    ),
    "purchasing": SchemaRanges(
        min_count=1_000,
        max_count=50_000,
        min_chaos=0.05,
        max_chaos=0.10,
        description="Combined header + detail volume per run — was 10k-1M/day",
    ),
    "warehouse": SchemaRanges(
        min_count=1_000,
        max_count=50_000,
        min_chaos=0.05,
        max_chaos=0.10,
        description="Stock movements per run — was 20k-1M/day",
    ),
    "humanresources": SchemaRanges(
        min_count=50,
        max_count=100,
        min_chaos=0.05,
        max_chaos=0.08,
        description="Small, stable HR dimension",
    ),
    "product": SchemaRanges(
        min_count=500,
        max_count=1_500,
        min_chaos=0.05,
        max_chaos=0.08,
        absolute_max=100_000,
        description="Product catalog with 100k absolute ceiling",
    ),
    "person": SchemaRanges(
        min_count=10_000,
        max_count=50_000,
        min_chaos=0.05,
        max_chaos=0.10,
        description="Customers proportional to sales volume, per run — min bumped 2026-08-21 for testing, was 1k-50k",
    ),
    "marketing": SchemaRanges(
        min_count=10,
        max_count=5_000,
        min_chaos=0.05,
        max_chaos=0.08,
        description="Campaigns and ads derived from SKUs and campaign periods — was 1k-500k/day",
    ),
}

# ── Per-table overrides (take priority over schema-level ranges) ──────────────

TABLE_RANGES: Dict[str, SchemaRanges] = {
    "sales.salesorder": SchemaRanges(
        min_count=20_000, max_count=200_000, min_chaos=0.05, max_chaos=0.10,
        description="Primary sales fact table, per ~10-15min run — bumped 2026-08-21 for testing, was 1k-50k, no absolute ceiling",
    ),
    "sales.sales_return_order": SchemaRanges(
        min_count=50, max_count=500, min_chaos=0.05, max_chaos=0.10,
        description="Returns (~2-5% of sales volume), per run — was 5k-50k/day, no absolute ceiling",
    ),
    "warehouse.transaction_history": SchemaRanges(
        min_count=1_000, max_count=50_000, min_chaos=0.05, max_chaos=0.10,
        description="Stock transaction history, per run — was 40k-1M/day, no absolute ceiling",
    ),
    "warehouse.product_inventory": SchemaRanges(
        min_count=100, max_count=1_000, min_chaos=0.05, max_chaos=0.08,
        description="Current inventory (one row per product per location), per run",
    ),
    "person.customer": SchemaRanges(
        min_count=500, max_count=2_000, min_chaos=0.05, max_chaos=0.10,
        description="Primary customer dimension, per run — dropped from 10k-50k to 500-2k 2026-09-05, table already ~140M rows and growth needed to slow down",
    ),
    "person.address": SchemaRanges(
        min_count=1_000, max_count=50_000, min_chaos=0.05, max_chaos=0.10,
        description="Addresses (1:N with customers), per run — was 20k-1M/day, no absolute ceiling",
    ),
    "product.product": SchemaRanges(
        min_count=500, max_count=1_500, min_chaos=0.05, max_chaos=0.08,
        absolute_max=100_000,
        description="Product catalog",
    ),
    "product.product_category": SchemaRanges(
        min_count=20, max_count=50, min_chaos=0.05, max_chaos=0.08,
        description="Stable product categories",
    ),
    "humanresources.employee": SchemaRanges(
        min_count=50, max_count=100, min_chaos=0.05, max_chaos=0.08,
        description="Employees — small and critical",
    ),
    "humanresources.department": SchemaRanges(
        min_count=10, max_count=20, min_chaos=0.05, max_chaos=0.08,
        description="Departments lookup table",
    ),
    "purchasing.purchaseorderheader": SchemaRanges(
        min_count=1_000, max_count=50_000, min_chaos=0.05, max_chaos=0.08,
        description="Purchase order headers, per run — was 10k-1M/day, no absolute ceiling. "
                     "Also used as the fan-out LIMIT for purchaseorderdetail/purchaseorderaudit "
                     "(see utils/cli_common.py + those two seeders) — this is the one entry "
                     "in this file that directly bounds those two, not just its own table.",
    ),
    "purchasing.purchaseorderdetail": SchemaRanges(
        min_count=1_000, max_count=50_000, min_chaos=0.05, max_chaos=0.10,
        description="Purchase order line items — fan-out of headers, not independently drawn "
                     "(this range is currently inert; kept in sync for when/if it gets its own absolute_max)",
    ),
    "marketing.marketing_campaign": SchemaRanges(
        min_count=100, max_count=5_000, min_chaos=0.05, max_chaos=0.08,
        description="Marketing campaigns",
    ),
    "marketing.marketing_daily_performance": SchemaRanges(
        min_count=100, max_count=5_000, min_chaos=0.05, max_chaos=0.08,
        description="Daily ad performance time-series, per run — was 10k-500k/day",
    ),
    "purchasing.vendor": SchemaRanges(
        min_count=20, max_count=300, min_chaos=0.05, max_chaos=0.08,
        description="Vendor master data — small, not a fact table",
    ),
    "warehouse.warehouselocation": SchemaRanges(
        min_count=10, max_count=200, min_chaos=0.05, max_chaos=0.08,
        description="Physical warehouse locations — small, not a fact table",
    ),
    "sales.seller": SchemaRanges(
        min_count=10, max_count=300, min_chaos=0.05, max_chaos=0.08,
        description="Marketplace sellers/stores — small, not a fact table",
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


# ── Mutation ranges (UPDATE/DELETE volume for the CDC mutator seeders) ────────
# Added 2026-08-22 for the future Debezium+Kafka CDC pipeline (dbapi/api/utils/mutator_common.py
# + <domain>/<table>_mutate.py + airflow/dags/nexomart/mutator/nexomart_mutator_dag.py, created
# disabled — not wired into nexomart_seeder_dag). Same min/max-range mechanism as SCHEMA_RANGES/
# TABLE_RANGES above, but sized for how many *existing* rows to touch per run, not how many new
# ones to insert — deliberately much smaller. `delete_ratio` is the share of the sampled rows
# that get deleted (or, for sales/audit tables, soft-flagged) instead of updated; the sales/audit
# no-hard-delete rule itself lives in each table's own mutate_*.py (`hard_delete_allowed=False`),
# not here — this file only sizes the volume.

@dataclass
class MutationRanges:
    min_count: int
    max_count: int
    delete_ratio: float
    description: str = ""


MUTATION_SCHEMA_RANGES: Dict[str, MutationRanges] = {
    "sales": MutationRanges(
        min_count=200, max_count=2_000, delete_ratio=0.15,
        description="Sales rows touched per run — DELETE here always resolves to a soft flag",
    ),
    "purchasing": MutationRanges(
        min_count=100, max_count=1_000, delete_ratio=0.20,
        description="Purchasing rows touched per run",
    ),
    "warehouse": MutationRanges(
        min_count=100, max_count=1_000, delete_ratio=0.20,
        description="Warehouse rows touched per run",
    ),
    "person": MutationRanges(
        min_count=200, max_count=2_000, delete_ratio=0.10,
        description="Person rows touched per run",
    ),
    "product": MutationRanges(
        min_count=50, max_count=300, delete_ratio=0.10,
        description="Product rows touched per run — mostly discontinuation-style updates",
    ),
    "marketing": MutationRanges(
        min_count=50, max_count=500, delete_ratio=0.15,
        description="Marketing rows touched per run",
    ),
    "humanresources": MutationRanges(
        min_count=10, max_count=50, delete_ratio=0.05,
        description="HR rows touched per run — small and sensitive, kept conservative",
    ),
}

MUTATION_TABLE_RANGES: Dict[str, MutationRanges] = {
    "sales.salesorder": MutationRanges(
        min_count=200, max_count=2_000, delete_ratio=0.15,
        description="Order status/payment-style updates; DELETE resolves to a soft flag",
    ),
    "sales.returnorder": MutationRanges(
        min_count=50, max_count=500, delete_ratio=0.15,
        description="Return status updates; DELETE resolves to a soft flag",
    ),
    "sales.ordereventlog": MutationRanges(
        min_count=20, max_count=200, delete_ratio=0.10,
        description="Event log corrections; DELETE resolves to a soft flag (audit-type table)",
    ),
    "purchasing.purchaseorderaudit": MutationRanges(
        min_count=50, max_count=500, delete_ratio=0.10,
        description="Audit trail corrections; DELETE resolves to a soft flag (audit-type table)",
    ),
    "purchasing.purchaseorderheader": MutationRanges(
        min_count=100, max_count=1_000, delete_ratio=0.15,
        description="PO status/date updates — real DELETE allowed (not itself an audit table)",
    ),
    "person.customer": MutationRanges(
        min_count=200, max_count=2_000, delete_ratio=0.10,
        description="Customer profile updates — real DELETE allowed (e.g. right-to-be-forgotten scenarios)",
    ),
}


def get_mutation_ranges(schema: str = None, table: str = None) -> MutationRanges:
    """Return mutation ranges for a table (highest priority) or schema, with a safe fallback."""
    if table and table in MUTATION_TABLE_RANGES:
        return MUTATION_TABLE_RANGES[table]
    if schema and schema in MUTATION_SCHEMA_RANGES:
        return MUTATION_SCHEMA_RANGES[schema]
    return MutationRanges(
        min_count=50, max_count=500, delete_ratio=0.10,
        description="Default fallback for unmapped schemas",
    )
