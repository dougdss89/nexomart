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
import random

from sqlalchemy import insert
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
