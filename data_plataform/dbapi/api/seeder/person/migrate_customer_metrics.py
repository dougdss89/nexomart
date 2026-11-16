"""One-shot migration: re-derive demographics.metrics for existing person.customer rows.

Rows seeded before the engagement-driven generator (samples/customer.py::_derive_metrics) have
independent, uncorrelated metrics. This rewrites only demographics.metrics (keeping the existing
date_of_first_purchase and any extra keys such as lifetime_value_score), normalizes customer_type
back to SC/VC (the old mutator had scrambled ~8k rows to individual/business/vip), and sets
updated_at = now() so incremental ingestion (COALESCE(updated_at, created_at) > watermark)
re-lands the rows.

Company vs individual is read from demographics.social: tax_regime is set only for companies and
occupation only for individuals (verified disjoint and exhaustive on the live table, 2026-11-14).

Connects straight to Postgres with asyncpg (host port 5433), so it runs from the host:

    set -a; source configs/secrets.env; set +a
    cd dbapi && uv run python api/seeder/person/migrate_customer_metrics.py --max-batches 1 --dry-run
    cd dbapi && uv run python api/seeder/person/migrate_customer_metrics.py

Resumable: pass --start-id with the next id printed by the last completed batch. For parallel
workers give each a disjoint --start-id/--end-id range (batch boundaries stay aligned to
--batch-size from --start-id, so use start ids that are multiples of the batch size).
"""

import argparse
import asyncio
import json
import os
import random
import time
from datetime import date

import asyncpg
from api.samples.customer import _derive_metrics

NEGATIVE_CHAOS_RATE = 0.02  # same 2% negative total_purchased bug the generator injects

SELECT_BATCH = """
SELECT customer_id,
       country_id,
       (demographics->'social'->>'tax_regime') IS NOT NULL AS is_company,
       demographics->'metrics'->>'date_of_first_purchase' AS first_purchase
FROM person.customer
WHERE customer_id >= $1 AND customer_id < $2
"""

UPDATE_BATCH = """
UPDATE person.customer c
SET demographics = jsonb_set(
        c.demographics,
        '{metrics}',
        COALESCE(c.demographics->'metrics', '{}'::jsonb) || t.metrics
    ),
    customer_type = t.customer_type,
    updated_at = now()
FROM tmp_customer_metrics t
WHERE c.customer_id = t.customer_id AND c.country_id = t.country_id
"""


def build_rows(records) -> list[tuple]:
    out = []
    for r in records:
        is_company = bool(r["is_company"])
        first = date.fromisoformat(r["first_purchase"]) if r["first_purchase"] else None
        metrics = _derive_metrics(is_company, first=first)
        if random.random() < NEGATIVE_CHAOS_RATE:
            metrics["total_purchased"] = -abs(metrics["total_purchased"])
        out.append(
            (
                r["customer_id"],
                r["country_id"],
                "VC" if is_company else "SC",
                json.dumps(metrics),
            )
        )
    return out


async def main(args) -> None:
    conn = await asyncpg.connect(
        host=args.host,
        port=args.port,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=args.database,
    )
    await conn.execute("SET synchronous_commit = off")
    max_id = await conn.fetchval("SELECT max(customer_id) FROM person.customer")
    print(f"max customer_id = {max_id:,} | batch = {args.batch_size:,} ids | dry_run = {args.dry_run}")

    await conn.execute(
        "CREATE TEMP TABLE tmp_customer_metrics "
        "(customer_id int, country_id int, customer_type text, metrics jsonb)"
    )

    lo, done, batches, t0 = args.start_id, 0, 0, time.time()
    stop = min(max_id, args.end_id) if args.end_id is not None else max_id
    while lo <= stop and (args.max_batches is None or batches < args.max_batches):
        hi = lo + args.batch_size
        records = await conn.fetch(SELECT_BATCH, lo, hi)
        rows = build_rows(records)
        if not args.dry_run and rows:
            async with conn.transaction():
                await conn.execute("TRUNCATE tmp_customer_metrics")
                await conn.copy_records_to_table(
                    "tmp_customer_metrics",
                    records=rows,
                    columns=["customer_id", "country_id", "customer_type", "metrics"],
                )
                status = await conn.execute(UPDATE_BATCH)
            updated = int(status.split()[-1])
        else:
            updated = 0
        done += len(rows)
        batches += 1
        elapsed = time.time() - t0
        print(
            f"batch ids [{lo:,}, {hi:,}): read {len(rows):,}, updated {updated:,} | "
            f"total {done:,} | {elapsed:,.0f}s | next --start-id {hi}",
            flush=True,
        )
        lo = hi

    await conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5433)
    parser.add_argument("--database", default="nexomarket_db")
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--start-id", type=int, default=0)
    parser.add_argument("--end-id", type=int, default=None, help="stop at this id (exclusive-ish), for parallel workers")
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="derive but do not write")
    asyncio.run(main(parser.parse_args()))
