"""
Shared engine for CDC mutation seeders — UPDATE/DELETE against rows that already
exist, as opposed to cli_common.py's insert-only seeders.

Why this exists: to exercise a future Debezium+Kafka CDC pipeline, a change-stream
consumer needs non-INSERT events too. These scripts sample already-seeded rows and
either UPDATE a few business columns or DELETE them, so the WAL/replication slot
carries a realistic mix of c/u/d operations instead of insert-only traffic.

Rule (user-specified, applies to every table in the sales schema and every
audit-type table — purchaseorderaudit, ordereventlog): never a real DELETE, only a
soft flag (AuditMixin.is_active = false). Every other table may take a real DELETE,
capped by --delete-ratio. Each mutate_*.py script declares this via
`hard_delete_allowed` when calling run_mutation().

Sampling reuses cli_common.sample_fk_ids's TABLESAMPLE SYSTEM approach (touches only
~sample_pct% of pages regardless of table size) generalized to arbitrary — including
composite — primary keys, since four of these tables are partitioned (salesorder,
customer, purchaseorderheader, transactionhistory, dailyperformance all have a
partition key folded into the PK). Row mutation then goes through SQLAlchemy 2.0's
"ORM bulk UPDATE/DELETE by primary key" (`session.execute(update(Model), rows)` /
`session.execute(delete(Model), rows)`), which compiles to one executemany() per
batch — no for-loop of individual statements, same bulk discipline CLAUDE.md
requires of the insert-side seeders.
"""

import random
from collections.abc import Callable
from datetime import UTC, datetime

from api.config.seeder_config import get_mutation_ranges
from sqlalchemy import and_, bindparam, delete, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

DEFAULT_BATCH_SIZE = 1_000


def add_mutation_args(parser, *, schema: str, table: str | None = None):
    """Adds the randomizable mutation-volume flags to an existing parser — mirrors
    cli_common.add_volume_args(), but for how many existing rows to touch this run
    rather than how many new rows to insert."""
    ranges = get_mutation_ranges(schema=schema, table=table)

    random_group = parser.add_argument_group("Random Mode (recommended for Airflow)")
    random_group.add_argument(
        "--random",
        action="store_true",
        help="Draws the row count within the configured range (min/max)",
    )
    random_group.add_argument(
        "--min-count",
        type=int,
        default=ranges.min_count,
        help=f"Minimum rows to mutate (default: {ranges.min_count:,})",
    )
    random_group.add_argument(
        "--max-count",
        type=int,
        default=ranges.max_count,
        help=f"Maximum rows to mutate (default: {ranges.max_count:,})",
    )
    random_group.add_argument(
        "--delete-ratio",
        type=float,
        default=ranges.delete_ratio,
        help=f"Share of the sampled rows that get deleted/flagged instead of updated (default: {ranges.delete_ratio:.0%})".replace(
            "%", "%%"
        ),
    )

    fixed_group = parser.add_argument_group("Fixed Mode (development/testing)")
    fixed_group.add_argument(
        "--count",
        type=int,
        default=None,
        help="FIXED row count (ignores randomization)",
    )

    general_group = parser.add_argument_group("General Settings")
    general_group.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size per UPDATE/DELETE executemany (default: {DEFAULT_BATCH_SIZE:,})",
    )
    return parser


def resolve_mutation_args(args, *, schema: str, table: str | None = None):
    """Returns (count, delete_ratio, batch_size) from a Namespace produced by add_mutation_args()."""
    if args.random or args.count is None:
        count = random.randint(args.min_count, args.max_count)
    else:
        count = args.count

    print(f"🎲 Mutation volume drawn: {count:,} rows | delete ratio: {args.delete_ratio:.0%}")
    return count, args.delete_ratio, args.batch_size


async def sample_active_pks(
    session: AsyncSession, model, limit: int, sample_pct: float = 5.0
) -> list[dict]:
    """Server-side random sample of primary-key tuples from active (is_active=true)
    rows — generalizes cli_common.sample_fk_ids to arbitrary, including composite,
    primary keys instead of a single FK column. Falls back to a plain bounded fetch
    if the sample comes back short (small/sparse table).
    """
    table = model.__table__
    pk_cols = [c.name for c in table.primary_key.columns]
    qualified = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
    col_list = ", ".join(f'"{c}"' for c in pk_cols)

    result = await session.execute(
        text(
            f"SELECT {col_list} FROM {qualified} TABLESAMPLE SYSTEM (:pct) WHERE is_active = true LIMIT :limit"
        ).bindparams(pct=sample_pct, limit=limit)
    )
    rows = [dict(zip(pk_cols, r, strict=False)) for r in result.all()]

    if len(rows) < limit:
        fallback = await session.execute(
            select(*[table.c[c] for c in pk_cols]).where(model.is_active.is_(True)).limit(limit)
        )
        rows = [dict(zip(pk_cols, r, strict=False)) for r in fallback.all()]

    return rows


def split_for_mutation(pk_rows: list[dict], delete_ratio: float) -> tuple[list[dict], list[dict]]:
    """Splits sampled PK rows into (update_rows, delete_rows) per delete_ratio."""
    if not pk_rows:
        return [], []
    shuffled = pk_rows[:]
    random.shuffle(shuffled)
    n_delete = int(len(shuffled) * delete_ratio)
    return shuffled[n_delete:], shuffled[:n_delete]


async def bulk_update(
    session: AsyncSession, model, rows: list[dict], batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """ORM bulk UPDATE by primary key. Each dict must carry every PK column plus
    whichever business columns are changing — columns left out keep their current
    value (and AuditMixin's updated_at=onupdate=func.now() still fires normally)."""
    if not rows:
        return 0
    for i in range(0, len(rows), batch_size):
        await session.execute(update(model), rows[i : i + batch_size])
    await session.commit()
    return len(rows)


async def bulk_soft_delete(
    session: AsyncSession, model, pk_rows: list[dict], batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """Flags rows inactive instead of deleting them — the only 'delete' allowed for
    sales.*/audit-type tables (see module docstring)."""
    if not pk_rows:
        return 0
    now = datetime.now(UTC)
    rows = [{**pk, "is_active": False, "updated_at": now} for pk in pk_rows]
    return await bulk_update(session, model, rows, batch_size=batch_size)


async def bulk_hard_delete(
    session: AsyncSession, model, pk_rows: list[dict], batch_size: int = DEFAULT_BATCH_SIZE
) -> int:
    """Real bulk DELETE by primary key. Never call this for sales.*/audit-type
    tables — use bulk_soft_delete() there instead.

    Unlike bulk_update() (ORM-enabled bulk UPDATE, supported), SQLAlchemy 2.0 does not support
    an executemany-style bulk DELETE through the ORM (`InvalidRequestError: Bulk ORM DELETE not
    supported right now`, confirmed live 2026-09-04 on purchase_purchaseorder_vendor_mutate.py)
    — its own error message points at the fix: run it at the Core level instead, via the
    session's underlying Connection.

    Core-level `delete(model)` has no ORM magic to turn a list of PK dicts into a WHERE clause
    the way the ORM bulk UPDATE path does, though — a first attempt that just swapped
    `session.execute` for `conn.execute` with no `.where()` compiled to a bare `DELETE FROM
    <table>` with the parameter dicts silently ignored (confirmed live via SQL echo: `[SQL:
    DELETE FROM purchasing.vendor] [parameters: [(), (), (), (), ()]]` — an unconditional
    delete of the *entire table*, only caught here because an FK constraint happened to block
    it; a table with no inbound FKs would have just silently lost every row). The WHERE clause
    has to be built explicitly, matching each PK column to a same-named bind parameter so
    executemany-style dicts bind correctly — generalizes to composite PKs (the partitioned
    tables) the same way sample_active_pks() above already does.
    """
    if not pk_rows:
        return 0
    pk_cols = [c.name for c in model.__table__.primary_key.columns]
    stmt = delete(model).where(and_(*(model.__table__.c[col] == bindparam(col) for col in pk_cols)))
    conn = await session.connection()
    for i in range(0, len(pk_rows), batch_size):
        await conn.execute(stmt, pk_rows[i : i + batch_size])
    await session.commit()
    return len(pk_rows)


async def run_mutation(
    session: AsyncSession,
    model,
    *,
    label: str,
    count: int,
    delete_ratio: float,
    batch_size: int,
    apply_updates: Callable[[list[dict]], list[dict]],
    hard_delete_allowed: bool,
    sample_pct: float = 5.0,
) -> None:
    """Shared UPDATE+DELETE driver — the only per-table logic a mutate_*.py script
    supplies is `apply_updates` (adds the new column values on top of the sampled PK
    dicts) and `hard_delete_allowed` (False forces bulk_soft_delete instead of a real
    DELETE, per the sales/audit rule).
    """
    pk_rows = await sample_active_pks(session, model, limit=count, sample_pct=sample_pct)
    if not pk_rows:
        print(f"⚠️ {label}: no active rows found to mutate — has the insert-side seeder run yet?")
        return

    update_rows, delete_rows = split_for_mutation(pk_rows, delete_ratio)

    updated = await bulk_update(session, model, apply_updates(update_rows), batch_size=batch_size)

    deleted = 0
    if delete_rows:
        if hard_delete_allowed:
            deleted = await bulk_hard_delete(session, model, delete_rows, batch_size=batch_size)
            verb = "hard-deleted"
        else:
            deleted = await bulk_soft_delete(session, model, delete_rows, batch_size=batch_size)
            verb = "soft-deleted (is_active=false)"
        print(f"🗑️  {label}: {deleted:,} rows {verb}")

    print(
        f"✅ {label}: {updated:,} updated, {deleted:,} {'deleted' if hard_delete_allowed else 'flagged'} "
        f"(sampled {len(pk_rows):,}/{count:,} requested)"
    )
