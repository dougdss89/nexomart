"""
Standardized CLI for the seeders — avoids repeating the same argparse block in every file.

Two flag families, depending on the table type (see CLAUDE.md):

- Volume-driven (facts/dimensions with free-form counts, e.g.: sales.salesorder, person.customer):
    use `add_volume_args()` + `resolve_volume_args()`.
    Exposes --random/--min-count/--max-count/--min-chaos/--max-chaos/--count/--chaos-rate/--batch-size/--no-chaos.
    With no flags at all, it already draws a volume within the configured range (default behavior
    for Airflow calls via BashOperator with no parameters).

- Chaos-only (fixed master data tables or fan-out derived from what's already in the database, where
    the count is not a free input — e.g.: product_category, marketing_campaign):
    use `add_chaos_args()` + `resolve_chaos_args()`.
    Exposes only --chaos-rate/--no-chaos.

Both use `get_ranges()` from `api.config.seeder_config` as the single source of truth for ranges
per schema/table.
"""

import argparse
import json
import random

from sqlalchemy import func, insert, select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from api.config.seeder_config import get_ranges

DEFAULT_BATCH_SIZE = 1_000
# asyncpg limits queries to 32767 bound parameters; a multi-row insert().values([...])
# sums (number of columns x number of rows in the batch). With the batch_size above, even wide
# tables (~20-30 columns) stay well below the limit. Don't raise this default without accounting for that.


def build_parser(description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )


def add_volume_args(
    parser: argparse.ArgumentParser,
    *,
    schema: str,
    table: str | None = None,
    default_batch_size: int = DEFAULT_BATCH_SIZE,
) -> argparse.ArgumentParser:
    """Adds the randomizable volume+chaos flag groups to an existing parser."""
    ranges = get_ranges(schema=schema, table=table)

    random_group = parser.add_argument_group("Random Mode (recommended for Airflow)")
    random_group.add_argument(
        "--random", action="store_true",
        help="Draws values within the configured ranges (min/max)",
    )
    random_group.add_argument(
        "--min-count", type=int, default=ranges.min_count,
        help=f"Minimum record volume (default: {ranges.min_count:,})",
    )
    random_group.add_argument(
        "--max-count", type=int, default=ranges.max_count,
        help=f"Maximum record volume (default: {ranges.max_count:,})",
    )
    random_group.add_argument(
        "--min-chaos", type=float, default=ranges.min_chaos,
        help=f"Minimum chaos rate (default: {ranges.min_chaos:.2%})".replace("%", "%%"),
    )
    random_group.add_argument(
        "--max-chaos", type=float, default=ranges.max_chaos,
        help=f"Maximum chaos rate (default: {ranges.max_chaos:.2%})".replace("%", "%%"),
    )

    fixed_group = parser.add_argument_group("Fixed Mode (development/testing)")
    fixed_group.add_argument(
        "--count", type=int, default=None,
        help="FIXED record count (ignores randomization)",
    )
    fixed_group.add_argument(
        "--chaos-rate", type=float, default=None,
        help="FIXED chaos rate (ignores randomization, 0.0 to 0.10)",
    )

    general_group = parser.add_argument_group("General Settings")
    general_group.add_argument(
        "--batch-size", type=int, default=default_batch_size,
        help=f"Batch size for insertion (default: {default_batch_size:,})",
    )
    general_group.add_argument(
        "--no-chaos", action="store_true",
        help="Disables chaos engineering (generates clean data only)",
    )

    return parser


def resolve_volume_args(args: argparse.Namespace, *, schema: str, table: str | None = None):
    """Returns (count, chaos_rate, apply_chaos, batch_size) from a Namespace produced by add_volume_args()."""
    ranges = get_ranges(schema=schema, table=table)

    if args.random or args.count is None:
        count = random.randint(args.min_count, args.max_count)
    else:
        count = args.count

    if args.no_chaos:
        chaos_rate = 0.0
    elif args.chaos_rate is not None:
        chaos_rate = args.chaos_rate
    elif args.random:
        chaos_rate = random.uniform(args.min_chaos, args.max_chaos)
    else:
        chaos_rate = ranges.min_chaos

    apply_chaos = chaos_rate > 0 and not args.no_chaos

    print(f"🎲 Volume drawn: {count:,} records | Chaos: {chaos_rate:.2%}")

    return count, chaos_rate, apply_chaos, args.batch_size


def add_chaos_args(parser: argparse.ArgumentParser, *, default_chaos_rate: float = 0.05) -> argparse.ArgumentParser:
    """Adds only --chaos-rate/--no-chaos, for seeders without a free-form count."""
    parser.add_argument(
        "--chaos-rate", type=float, default=default_chaos_rate,
        help=f"Chaos rate (default: {default_chaos_rate:.2%})".replace("%", "%%"),
    )
    parser.add_argument(
        "--no-chaos", action="store_true",
        help="Disables chaos engineering (generates clean data only)",
    )
    return parser


def resolve_chaos_args(args: argparse.Namespace, default_chaos_rate: float = 0.05):
    """Returns (chaos_rate, apply_chaos) from a Namespace produced by add_chaos_args()."""
    chaos_rate = 0.0 if args.no_chaos else args.chaos_rate
    apply_chaos = chaos_rate > 0 and not args.no_chaos
    return chaos_rate, apply_chaos


async def generate_unique_business_ids(
    session: AsyncSession,
    id_column,
    *,
    prefix: str,
    count: int,
    digits: int = 9,
) -> list[str]:
    """Generates `count` unique "{prefix}-{digits-digit number}" IDs that collide with
    neither each other nor existing rows in `id_column`'s table.

    Root-cause fix for the transactional-ID pattern in CLAUDE.md (SO-/PO-/RO-/TR- prefixes,
    9 random digits = ~900M values): seeders used to draw each ID independently with
    random.randint() and let the batch INSERT fail on collision. With batch sizes up to
    10,000 rows, the birthday-paradox math makes that failure the common case once a table
    passes tens of thousands of rows — not a rare edge case (incident 2026-08-13: sales_order
    and purchaseorderheader batches were failing outright, previously masked by a since-fixed
    false-success bug that swallowed the IntegrityError).

    random.sample() (without replacement) guarantees no intra-batch duplicates; the single
    IN-query against the DB catches the much rarer cross-batch collision, and only the
    handful of colliding candidates get regenerated instead of the whole batch.
    """
    low, high = 10 ** (digits - 1), 10 ** digits - 1
    chosen: set[str] = set()
    while len(chosen) < count:
        needed = count - len(chosen)
        sampled = random.sample(range(low, high + 1), needed)
        candidates = {f"{prefix}-{n}" for n in sampled} - chosen
        if not candidates:
            continue
        existing = (
            await session.execute(select(id_column).where(id_column.in_(candidates)))
        ).scalars().all()
        chosen |= candidates - set(existing)
    return list(chosen)


DEFAULT_FK_SAMPLE_SIZE = 5_000
# How many real FK values to pull server-side when a seeder needs to pick one at random
# per generated row (e.g. sales_order.py assigning a customer_id to each new order).


async def sample_fk_ids(
    session: AsyncSession, column, limit: int = DEFAULT_FK_SAMPLE_SIZE, sample_pct: float = 2.0
) -> list:
    """Server-side random sample of real, existing values from `column` — bounded regardless
    of how large the source table grows.

    Root-cause fix (2026-08-17, revised same day): seeders used to do
    `select(column)).scalars().all()` to load the *entire* column into Python just to
    `random.choice()` from it per generated row — cost grows with total accumulated history,
    not with rows needed this run. First attempt replaced it with `ORDER BY random() LIMIT n`,
    which is a well-known Postgres trap: it still has to compute a random sort key for *every*
    row and sort them before taking the top n, i.e. it still scans (and sorts!) the whole
    table — measured 2min+ and still running against ~2M rows, no better than the original bug.
    `TABLESAMPLE SYSTEM (pct)` is the actual fix: it picks random *blocks* and returns
    whatever rows are in them, touching only ~pct% of the table's pages regardless of table
    size (measured 52ms for 5,000 rows out of 2M). Every value returned is still a real,
    existing row — the fix only changes where the sampling happens, never what's sampled.
    Falls back to a plain bounded fetch if the sample comes back empty (small/sparse table —
    exactly the case where a full read isn't a scaling concern anyway).
    """
    table = column.table
    qualified = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
    result = await session.execute(
        text(f'SELECT "{column.name}" FROM {qualified} TABLESAMPLE SYSTEM (:pct) LIMIT :limit')
        .bindparams(pct=sample_pct, limit=limit)
    )
    ids = [row[0] for row in result.all()]
    if not ids:
        ids = (await session.execute(select(column).limit(limit))).scalars().all()
    return ids


async def skip_if_already_seeded(session: AsyncSession, model, *, label: str) -> bool:
    """Returns True (and prints a skip message) if `model` already has any rows.

    Root-cause fix for the "false success" pattern found across the fixed/static master-data
    seeders (org chart, job roles, product categories/models, sales channels, transaction
    types, marketing channels, countries, states, special offers): those seeders insert a
    fixed payload (explicit PKs or a unique name/code) every DAG run with no idempotency
    check, so every run after the first hits a PK/unique collision. Some of them then
    swallowed that IntegrityError (except: print + no raise) and still exited 0 — Airflow saw
    "success" even though zero rows were written that run (incident 2026-08-13: audit found
    this on master_data.hr_department, hr_job_role, product_category, product_model,
    sales_channel, warehouse_transaction_type — all "success" with a duplicate-key error
    logged underneath). Since this data is meant to be seeded once, the correct behavior on
    a re-run isn't to fail (nor to silently succeed while doing nothing) — it's to detect
    "already there" and skip cleanly, while still raising on any other, genuinely unexpected
    error.

    Deliberately checks "any row present" rather than an exact/expected count: some of these
    generators aren't deterministic between calls (e.g. humanresources_job_role.py rolls a
    random per-title history flag, so the row count it would generate this run can differ
    from what's already in the DB from a previous run) — comparing against a freshly
    recomputed "expected" count is fragile for exactly that reason, and master data doesn't
    need row-perfect reconciliation, just "don't try to insert the fixed set twice."
    """
    existing = await session.scalar(select(func.count()).select_from(model)) or 0
    if existing > 0:
        print(f"✅ {label} already seeded ({existing:,} rows) — skipping.")
        return True
    return False


async def insert_in_batches(session: AsyncSession, model, rows: list[dict], batch_size: int = DEFAULT_BATCH_SIZE) -> int:
    """Inserts `rows` in batches of `batch_size` and commits at the end.

    Fan-out seeders (category 2) derive their count from what's already in the
    database (e.g.: N items per real header) — the total can easily exceed
    tens of thousands of rows once the parent dimensions are well populated, and a
    single unbatched `insert().values(rows)` blows past asyncpg's 32767-parameter
    limit. Use this instead of a direct insert whenever `rows` doesn't have a
    known, small ceiling.
    """
    if not rows:
        return 0
    for i in range(0, len(rows), batch_size):
        stmt = insert(model).values(rows[i:i + batch_size])
        await session.execute(stmt)
    await session.commit()
    return len(rows)


async def bulk_copy(session: AsyncSession, model, rows: list[dict]) -> int:
    """Loads `rows` via Postgres COPY (asyncpg's copy_records_to_table) instead of INSERT.

    Solution B from the 2026-08-13 performance proposal: COPY streams rows over the wire
    protocol without per-statement planning/logging, well ahead of even batched multi-row
    INSERTs at the 10^5-10^6 rows/run volumes the highest-traffic seeders (sales_order,
    transaction_history, purchaseorderheader/detail, customer, daily_performance) now draw
    per CLAUDE.md's dynamic ranges. For ordinary fan-out volumes insert_in_batches() is
    simpler and plenty fast — reserve this for the seeders that actually need it.

    Column set is taken from the model's Table, restricted to keys present in rows[0] (same
    convention as insert_in_batches()/insert().values()). JSONB columns are json.dumps()'d
    when not already a str: asyncpg's COPY path has no SQLAlchemy-level JSON adapter in front
    of it, so a raw dict would otherwise be handed to the wire protocol as an untyped value.

    Columns absent from rows[0] but carrying a Python-side `default` (e.g. AuditMixin's
    row_version=1, is_active=True — no server_default behind either) are filled in here
    explicitly, one call per row for callables. COPY talks straight to asyncpg and skips
    SQLAlchemy's insert-compilation step entirely, so those defaults would otherwise never
    apply and the column would go in as NULL instead (caught 2026-08-13: person_customer.py
    never sets row_version, which insert().values() silently defaulted but bulk_copy() sent
    as NULL, tripping the NOT NULL constraint). Same col.default.arg(...) call Base.from_pydantic()
    already uses for the same reason. Columns with only a server_default (e.g. created_at)
    are deliberately left out of this — Postgres applies those on its own.
    """
    if not rows:
        return 0
    table = model.__table__
    jsonb_columns = {c.name for c in table.columns if isinstance(c.type, JSONB)}

    present_columns = [c.name for c in table.columns if c.name in rows[0]]
    defaulted_columns = [c for c in table.columns if c.name not in rows[0] and c.default is not None]
    columns = present_columns + [c.name for c in defaulted_columns]

    records = []
    for row in rows:
        record = [
            json.dumps(row[col]) if col in jsonb_columns and not isinstance(row[col], str) else row[col]
            for col in present_columns
        ]
        record.extend(
            c.default.arg(None) if c.default.is_callable else c.default.arg
            for c in defaulted_columns
        )
        records.append(tuple(record))

    raw_conn = await session.connection()
    dbapi_conn = await raw_conn.get_raw_connection()
    asyncpg_conn = dbapi_conn.driver_connection

    await asyncpg_conn.copy_records_to_table(
        table.name,
        schema_name=table.schema,
        records=records,
        columns=columns,
    )
    await session.commit()
    return len(rows)
