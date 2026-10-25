"""Backfill: force a full re-land of specific tables into the nexomart landing zone.

ingestion_common's ingestion is incremental: a table is skipped once its watermark
(ingestion_control.watermarks) is past all its rows. If a table's landing data is lost while
its watermark stays advanced (e.g. the landing bucket was wiped, or an earlier run advanced the
watermark without the parts surviving), a normal run never re-lands it -- it reads 0 rows and
writes nothing, so no part files and no date folder are ever recreated.

Static dimension tables are the usual victims: they get no new rows to trip the
COALESCE(updated_at, created_at) > watermark filter, so they stay missing. Fact tables
self-heal because fresh rows keep landing on every run.

This resets the watermark for each named table to EPOCH and runs a full ingest_table, re-landing
every row. ingest_table advances the watermark again afterwards, so normal incremental ingestion
resumes with no further changes. Reuses the exact ingest_table path the DAG uses -- no separate
landing logic to drift.

Usage (inside the airflow-worker venv, same convention as the ingestion DAG):
    PYTHONPATH=/dockervol:/dockervol/dbapi uv run --project /dockervol \
      python /dockervol/dbapi/api/seeder/ingestions/backfill_ingestion.py \
      --tables product.product,product.productembedding,person.countryregion
"""

import argparse
from datetime import UTC, datetime

import psycopg2.extras
from api.seeder.ingestions.ingestion_common import (
    DEFAULT_CHUNKSIZE,
    EPOCH,
    _sync_database_url,
    ingest_table,
    set_watermark,
)
from sqlalchemy import create_engine


def _parse_targets(raw: str) -> list[tuple[str, str]]:
    targets: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if "." not in item:
            raise SystemExit(f"--tables entry must be schema.table, got: {item!r}")
        schema, table = item.split(".", 1)
        targets.append((schema, table))
    if not targets:
        raise SystemExit("--tables is empty")
    return targets


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Force a full re-land of specific tables into the nexomart landing zone"
    )
    parser.add_argument(
        "--tables",
        required=True,
        help="Comma-separated schema.table list (e.g. product.product,person.countryregion)",
    )
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE)
    args = parser.parse_args()

    targets = _parse_targets(args.tables)
    engine = create_engine(_sync_database_url())
    raw_conn = engine.raw_connection()
    # Same no-op jsonb/json loaders as ingest_schema: keep JSONB columns as the raw text Postgres
    # already sent so the normalize step skips a wasted decode+re-encode round-trip.
    psycopg2.extras.register_default_jsonb(raw_conn.dbapi_connection, loads=lambda raw: raw)
    psycopg2.extras.register_default_json(raw_conn.dbapi_connection, loads=lambda raw: raw)
    try:
        now = datetime.now(UTC)
        run_date = now.strftime("%Y-%m-%d")
        run_ts = now.strftime("%Y%m%dT%H%M%SZ")
        print(f"Backfill of {len(targets)} table(s), run_ts={run_ts}")

        grand_total = 0
        for schema, table in targets:
            set_watermark(engine, schema, table, EPOCH)  # force a full scan on this table
            rows = ingest_table(
                raw_conn, engine, schema, table, run_date, run_ts, chunksize=args.chunksize
            )
            grand_total += rows
            print(f"OK {schema}.{table}: {rows:,} rows re-landed")

        print(f"Backfill done: {grand_total:,} rows across {len(targets)} table(s)")
    except Exception as e:
        print(f"Backfill failed: {e}")
        raise
    finally:
        raw_conn.close()
        engine.dispose()


if __name__ == "__main__":
    main()
