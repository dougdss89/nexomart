"""Compacts the nexomart landing zone's day folders so they stop growing without bound.

Why: every ingestion run appends one part per day folder it touches, and `created_at`/event dates
are spread over 2020->now, so a run touches nearly every day folder. Folders end up with ~one
object per run (200 to 1,100+ after two months, ~9M objects overall). MinIO stores each object as a
directory tree on a spinning disk, so listing/reading the landing cold costs several seeks per
object (Spark's Bronze listing stage of person.customer: ~90 s per task, 2026-11-20).

Levels, per `<schema>/<table>/<YYYY-MM-DD>/` folder (object name prefix = level):
  part-*     L0  written by ingestion, one per run.
  compact-*  L1  the merge of >= --min-parts L0 parts.
  base-*     L2  the merge of >= --fold-after L1 files (plus the previous base).
A folder is therefore bounded by ~(min-parts + fold-after + grace window) objects instead of one
per run, and an object is rewritten at most once per level. The very first compaction of a folder
that has no L1/L2 yet (the existing backlog) goes straight to a base.

Never merged into a bigger file: objects >= TARGET_BYTES; a group's compressed size is capped there.
The landing keeps every version of a row, so merging never deduplicates (the Bronze incremental
filter and the manifest depend on it).

Grace window (--grace-hours, default 48): only parts from runs older than this are merged. The
Bronze IncrementalReader reads the paths listed in `ingestion_control.landing_runs` (manifest) of
runs newer than what Bronze already holds, and falls back to the full glob when the first of them
is older than the window, so a manifest path can never point at a merged-away part.

How work is found without listing the landing:
1. `ingest_table` counts parts per day folder in `ingestion_control.landing_parts`.
2. The first run that reaches a table registers its existing day folders with an unknown count
   (NULL) -- one directory listing per table (~100 s cold for 2.5k folders), counted against
   --max-seconds. A NULL folder is resolved by listing that one folder.
3. Candidates: `parts >= --min-parts`, `compacts >= --fold-after` or unknown, fullest first,
   round-robin across tables so a huge one doesn't starve the others.

Safety:
- The merged file is written and its size checked before anything is deleted. Right before the
  delete, the sources are written to `ingestion_control.landing_compaction_journal` and the next run
  finishes an interrupted delete first, so killing the process mid-folder leaves no duplicate rows.
- Postgres advisory lock: only one run at a time (a second exits immediately).
- `--max-seconds` bounds a run (checked between folders; a folder in flight is never interrupted).
  0 = no limit, for a manual drain. `--workers N` merges N folders concurrently.

Usage (inside the airflow-worker venv, same convention as the ingestion DAG):
    PYTHONPATH=/dockervol:/dockervol/dbapi uv run --project /dockervol \
      python /dockervol/dbapi/api/seeder/ingestions/compact_landing.py \
      [--schemas person,sales] [--tables person.customer] [--min-parts 10] [--fold-after 10] \
      [--grace-hours 48] [--max-seconds 600] [--workers 1] [--dry-run]
"""

import argparse
import io
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from itertools import zip_longest

import pyarrow as pa
import pyarrow.parquet as pq
import urllib3
from api.config import settings
from api.seeder.ingestions.ingestion_common import (
    LANDING_BUCKET,
    LANDING_PARTS_TABLE,
    LANDING_PREFIX,
    LANDING_RUNS_TABLE,
    WATERMARK_SCHEMA,
    _ensure_landing_parts_table,
    _ensure_landing_runs_table,
    _sync_database_url,
    _write_parquet_with_retry,
    list_schema_tables,
)
from minio.deleteobjects import DeleteObject
from sqlalchemy import create_engine, text

from minio import Minio

SCHEMAS = ["humanresources", "marketing", "person", "product", "purchasing", "sales", "warehouse"]
DEFAULT_MIN_PARTS = 10
DEFAULT_FOLD_AFTER = 10
DEFAULT_GRACE_HOURS = 48
DEFAULT_MAX_SECONDS = 600
DEFAULT_PRUNE_RUNS_DAYS = 90
KEEP_RUNS_PER_TABLE = 50  # newest manifests per table that pruning never removes
# Objects at or above this size are already "big enough" and are left alone; merged groups are
# capped at it (compressed bytes), which keeps one group's decoded size well inside the worker's
# memory limit.
TARGET_BYTES = 128 * 1024 * 1024

COUNTER = f'"{WATERMARK_SCHEMA}"."{LANDING_PARTS_TABLE}"'
RUNS = f'"{WATERMARK_SCHEMA}"."{LANDING_RUNS_TABLE}"'
JOURNAL = f'"{WATERMARK_SCHEMA}"."landing_compaction_journal"'
# Row marking that a table's existing day folders were registered. parts = 0 keeps it out of the
# candidate query.
BOOTSTRAP_DAY = "_bootstrap"
# Postgres advisory-lock key: only one compactor may run at a time. Two of them walk the folders in
# the same order, so they would merge the same folder twice and duplicate its rows (a manual drain
# overlapped the DAG task on 2026-11-28; nothing was written, but only by luck).
LOCK_KEY = 4_100_000_001

_RUN_TS = re.compile(r"-(\d{8}T\d{6}Z)-")


@dataclass(frozen=True)
class Obj:
    """The two fields of a MinIO object the merge logic needs (also used for files we just wrote)."""

    object_name: str
    size: int


@dataclass(frozen=True)
class Settings:
    min_parts: int
    fold_after: int
    grace_limit: str  # run_ts (YYYYmmddTHHMMSSZ) of the newest part allowed to be merged
    grace_hours: int


def _slow_disk_client(workers: int = 1) -> Minio:
    """The shared client (configs/api/minio.py) gives up after a 60 s read timeout and never
    retries, which suits uploads but not this script's calls: on MinIO's spinning disk a single
    directory listing page can take over a minute when the cache is cold or the disk is busy
    (seen 2026-11-28: ReadTimeoutError listing person/customer_email crashed the DAG task before
    it compacted anything). Listing, reading, stat and delete use this client instead."""
    return Minio(
        endpoint=settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=settings.MINIO_SECURE,
        http_client=urllib3.PoolManager(
            maxsize=max(10, workers * 2),
            timeout=urllib3.Timeout(connect=10, read=600),
            retries=urllib3.Retry(total=3, backoff_factor=5, status_forcelist=(500, 502, 503, 504)),
        ),
    )


def _ensure_journal(engine) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS {JOURNAL} (
                    compact_object text PRIMARY KEY,
                    sources text[] NOT NULL,
                    created_at timestamptz NOT NULL DEFAULT now()
                )
            """)
        )


def recover_journal(engine, client) -> int:
    """Finishes a delete that a killed run left half done: the merged file was already verified
    before its sources were journaled, so deleting whichever sources are still there is safe."""
    with engine.connect() as conn:
        pending = conn.execute(text(f"SELECT compact_object, sources FROM {JOURNAL}")).fetchall()
    for compact_object, sources in pending:
        try:
            client.stat_object(LANDING_BUCKET, compact_object)
            errors = list(
                client.remove_objects(LANDING_BUCKET, [DeleteObject(name) for name in sources])
            )
            if errors:
                raise RuntimeError(f"{len(errors)} sources still not deleted: {errors[:3]}")
            print(f"Recovered interrupted compaction: {compact_object} ({len(sources)} sources)")
        except Exception as e:
            # The merged file is missing or the delete fails: keep the journal row, don't guess.
            print(f"FAILED recovering {compact_object}: {type(e).__name__}: {e}")
            continue
        with engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM {JOURNAL} WHERE compact_object = :o"), {"o": compact_object}
            )
    return len(pending)


def register_day_folders(engine, client, prefix: str, schema: str, table: str) -> int:
    """One-time per table: gives every day folder that already exists in the landing a counter row
    (NULL = unknown count). Folders created afterwards are registered by `record_parts` during
    ingestion, so this listing is never repeated -- it isn't cheap: listing 2,519 day folders of
    person.customer took ~100 s on a cold cache (MinIO checks each prefix on its spinning disk)."""
    with engine.connect() as conn:
        done = conn.execute(
            text(f"""
                SELECT 1 FROM {COUNTER}
                WHERE schema_name = :schema AND table_name = :table AND day = :day
            """),
            {"schema": schema, "table": table, "day": BOOTSTRAP_DAY},
        ).first()
    if done:
        return 0
    base = f"{prefix}/{schema}/{table}/"
    days = sorted(
        o.object_name[len(base) :].strip("/")
        for o in client.list_objects(LANDING_BUCKET, prefix=base)
        if o.is_dir
    )
    with engine.begin() as conn:
        rows = [{"schema": schema, "table": table, "day": d, "parts": None} for d in days]
        rows.append({"schema": schema, "table": table, "day": BOOTSTRAP_DAY, "parts": 0})
        conn.execute(
            text(f"""
                INSERT INTO {COUNTER} (schema_name, table_name, day, parts)
                VALUES (:schema, :table, :day, :parts)
                ON CONFLICT (schema_name, table_name, day) DO NOTHING
            """),
            rows,
        )
    return len(days)


def candidate_days(engine, schema: str, table: str, cfg: Settings) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"""
                SELECT day FROM {COUNTER}
                WHERE schema_name = :schema AND table_name = :table AND day <> :bootstrap
                  AND (parts IS NULL OR parts >= :min_parts OR compacts >= :fold_after)
                  AND (retry_after IS NULL OR retry_after <= now())
                ORDER BY parts DESC NULLS LAST, day
            """),
            {
                "schema": schema,
                "table": table,
                "bootstrap": BOOTSTRAP_DAY,
                "min_parts": cfg.min_parts,
                "fold_after": cfg.fold_after,
            },
        )
        return [r[0] for r in rows]


def _set_state(
    engine,
    schema: str,
    table: str,
    day: str,
    *,
    parts: int | None,
    compacts: int,
    removed: int = 0,
    retry_hours: int | None = None,
) -> None:
    """`parts` is the L0 count the folder listing just showed (None marks the folder unknown
    again). After a merge a known counter is lowered by the number of parts removed instead of
    being overwritten, so a part an ingestion wrote meanwhile is not lost."""
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                UPDATE {COUNTER}
                SET parts = CASE
                        WHEN :parts IS NULL THEN NULL
                        WHEN parts IS NULL OR :removed = 0 THEN :parts
                        ELSE GREATEST(parts - :removed, 0)
                    END,
                    compacts = :compacts,
                    retry_after = CASE WHEN :retry_hours IS NULL THEN NULL
                                       ELSE now() + make_interval(hours => :retry_hours) END,
                    updated_at = now()
                WHERE schema_name = :schema AND table_name = :table AND day = :day
            """),
            {
                "schema": schema,
                "table": table,
                "day": day,
                "parts": parts,
                "compacts": compacts,
                "removed": removed,
                "retry_hours": retry_hours,
            },
        )


def _group_by_size(objs: list[Obj], cap: int) -> list[list[Obj]]:
    groups: list[list[Obj]] = []
    current: list[Obj] = []
    size = 0
    for obj in sorted(objs, key=lambda o: o.object_name):
        if current and size + obj.size > cap:
            groups.append(current)
            current, size = [], 0
        current.append(obj)
        size += obj.size
    if current:
        groups.append(current)
    return groups


def _merge_group(client, group: list[Obj]) -> tuple[bytes, int]:
    """Reads every object of `group` and returns (merged Parquet bytes, row count). Parts written
    at different times can disagree on a column that was entirely NULL in one of them (Arrow type
    `null`) or on a column that didn't exist yet, which the permissive promotion fills in."""
    tables = []
    rows = 0
    for obj in group:
        response = client.get_object(LANDING_BUCKET, obj.object_name)
        try:
            data = response.read()
        finally:
            response.close()
            response.release_conn()
        table = pq.read_table(io.BytesIO(data))
        tables.append(table)
        rows += table.num_rows
    merged = pa.concat_tables(tables, promote_options="permissive")
    if merged.num_rows != rows:
        raise RuntimeError(f"row count changed while merging: {rows} -> {merged.num_rows}")
    buffer = io.BytesIO()
    pq.write_table(merged, buffer, compression="snappy")
    return buffer.getvalue(), rows


def _merge_into(engine, client, name: str, group: list[Obj]) -> tuple[Obj, int]:
    """Writes `name` from `group`, verifies it, journals the sources and deletes them."""
    data, rows = _merge_group(client, group)
    _write_parquet_with_retry(name, data)
    if client.stat_object(LANDING_BUCKET, name).size != len(data):
        raise RuntimeError(f"{name}: stored size differs from what was uploaded")
    sources = [o.object_name for o in group]
    with engine.begin() as conn:
        conn.execute(
            text(f"""
                INSERT INTO {JOURNAL} (compact_object, sources) VALUES (:o, :s)
                ON CONFLICT (compact_object) DO UPDATE SET sources = EXCLUDED.sources
            """),
            {"o": name, "s": sources},
        )
    errors = list(client.remove_objects(LANDING_BUCKET, [DeleteObject(n) for n in sources]))
    if errors:
        # Leave the journal row: recover_journal retries the delete on the next run.
        raise RuntimeError(
            f"{name} written but {len(errors)} merged sources were not deleted "
            f"(rows are duplicated until they are): {errors[:3]}"
        )
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {JOURNAL} WHERE compact_object = :o"), {"o": name})
    return Obj(name, len(data)), rows


def _basename(obj) -> str:
    return obj.object_name.rsplit("/", 1)[1]


def compact_folder(
    engine,
    client,
    prefix: str,
    schema: str,
    table: str,
    day: str,
    run_ts: str,
    cfg: Settings,
    dry_run: bool,
) -> dict:
    """Returns {"objects": listed, "removed": n, "created": n, "rows": n, "folded": bool}."""
    folder = f"{prefix}/{schema}/{table}/{day}/"
    objs = [
        Obj(o.object_name, o.size)
        for o in client.list_objects(LANDING_BUCKET, prefix=folder)
        if o.object_name.endswith(".parquet")
    ]
    l0 = [o for o in objs if _basename(o).startswith("part-")]
    l1 = [o for o in objs if _basename(o).startswith("compact-")]
    l2 = [o for o in objs if _basename(o).startswith("base-")]
    small0 = [o for o in l0 if o.size < TARGET_BYTES]

    def part_run_ts(o: Obj) -> str:
        m = _RUN_TS.search(_basename(o))
        return m.group(1) if m else "~"  # unparsable name sorts after every run_ts: never merged

    eligible = [o for o in small0 if part_run_ts(o) <= cfg.grace_limit]
    result = {"objects": len(objs), "removed": 0, "created": 0, "rows": 0, "folded": False}

    merge_parts = len(eligible) >= cfg.min_parts
    # A folder with no merged file yet (the pre-existing backlog) merges straight into a base.
    parts_level = "base" if not l1 and not l2 else "compact"
    fold = len(l1) + (1 if merge_parts and parts_level == "compact" else 0) >= cfg.fold_after

    if dry_run:
        if merge_parts:
            result["removed"] += len(eligible)
            result["created"] += len(_group_by_size(eligible, TARGET_BYTES))
        result["folded"] = fold
        return result

    index = 0
    removed_l0 = 0
    l1_now = list(l1)
    try:
        if merge_parts:
            for group in _group_by_size(eligible, TARGET_BYTES):
                if len(group) < 2:
                    continue
                name = f"{folder}{parts_level}-{run_ts}-{index:05d}.parquet"
                index += 1
                new, rows = _merge_into(engine, client, name, group)
                removed_l0 += len(group)
                result["removed"] += len(group)
                result["created"] += 1
                result["rows"] += rows
                if parts_level == "compact":
                    l1_now.append(new)
        small_l1 = [o for o in l1_now if o.size < TARGET_BYTES]
        small_l2 = [o for o in l2 if o.size < TARGET_BYTES]
        if len(l1_now) >= cfg.fold_after and len(small_l1) + len(small_l2) >= 2:
            for group in _group_by_size(small_l1 + small_l2, TARGET_BYTES):
                if len(group) < 2:
                    continue
                name = f"{folder}base-{run_ts}-{index:05d}.parquet"
                index += 1
                _, rows = _merge_into(engine, client, name, group)
                folded = {o.object_name for o in group}
                l1_now = [o for o in l1_now if o.object_name not in folded]
                result["removed"] += len(group)
                result["created"] += 1
                result["folded"] = True
    except Exception:
        # Whatever state the folder is in now, the stored counts can't be trusted: re-list next run.
        _set_state(engine, schema, table, day, parts=None, compacts=0, removed=removed_l0)
        raise

    young = len(small0) - len(eligible) > 0
    _set_state(
        engine,
        schema,
        table,
        day,
        parts=len(l0) - removed_l0,
        compacts=len(l1_now),
        removed=removed_l0,
        # Young parts become mergeable once the grace window passes; don't re-list until then.
        retry_hours=max(1, cfg.grace_hours // 2) if young and removed_l0 == 0 else None,
    )
    return result


def prune_runs(engine, days: int) -> int:
    """Drops manifests older than `days`, always keeping each table's newest KEEP_RUNS_PER_TABLE
    so a table that went quiet can still be anchored by the Bronze reader."""
    if days <= 0:
        return 0
    with engine.begin() as conn:
        return conn.execute(
            text(f"""
                DELETE FROM {RUNS} r
                WHERE r.created_at < now() - make_interval(days => :days)
                  AND (SELECT count(*) FROM {RUNS} n
                       WHERE n.schema_name = r.schema_name AND n.table_name = r.table_name
                         AND n.to_wm > r.to_wm) >= :keep
            """),
            {"days": days, "keep": KEEP_RUNS_PER_TABLE},
        ).rowcount


def _targets(engine, schemas: list[str], tables: str) -> list[tuple[str, str]]:
    if tables:
        found = []
        for item in tables.split(","):
            item = item.strip()
            if not item:
                continue
            if "." not in item:
                raise SystemExit(f"--tables entry must be schema.table, got: {item!r}")
            found.append(tuple(item.split(".", 1)))
        return found  # type: ignore[return-value]
    return [(s, t) for s in schemas for t in list_schema_tables(engine, s)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact the nexomart landing zone's day folders")
    parser.add_argument("--schemas", default=",".join(SCHEMAS), help="Comma-separated schemas")
    parser.add_argument(
        "--tables", default="", help="Comma-separated schema.table list (overrides --schemas)"
    )
    parser.add_argument("--min-parts", type=int, default=DEFAULT_MIN_PARTS)
    parser.add_argument("--fold-after", type=int, default=DEFAULT_FOLD_AFTER)
    parser.add_argument("--grace-hours", type=int, default=DEFAULT_GRACE_HOURS)
    parser.add_argument(
        "--max-seconds",
        type=int,
        default=DEFAULT_MAX_SECONDS,
        help="Stop starting new folders after this long; 0 = no limit",
    )
    parser.add_argument("--workers", type=int, default=1, help="Folders merged concurrently")
    parser.add_argument("--prune-runs-days", type=int, default=DEFAULT_PRUNE_RUNS_DAYS)
    parser.add_argument("--prefix", default=LANDING_PREFIX, help="Landing prefix (tests only)")
    parser.add_argument("--dry-run", action="store_true", help="List what would be merged")
    args = parser.parse_args()

    engine = create_engine(_sync_database_url(), pool_size=max(5, args.workers + 3))
    client = _slow_disk_client(args.workers)
    run_ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    cfg = Settings(
        min_parts=args.min_parts,
        fold_after=args.fold_after,
        grace_limit=(datetime.now(UTC) - timedelta(hours=args.grace_hours)).strftime(
            "%Y%m%dT%H%M%SZ"
        ),
        grace_hours=args.grace_hours,
    )
    started = time.monotonic()

    def out_of_budget() -> bool:
        return bool(args.max_seconds) and time.monotonic() - started >= args.max_seconds

    lock_conn = None
    try:
        if not args.dry_run:
            lock_conn = engine.connect()
            if not lock_conn.execute(
                text("SELECT pg_try_advisory_lock(:k)"), {"k": LOCK_KEY}
            ).scalar():
                print("Another compact_landing run holds the lock; exiting without doing anything")
                return
            lock_conn.commit()  # the session-level lock outlives the transaction
        _ensure_landing_parts_table(engine)
        _ensure_landing_runs_table(engine)
        _ensure_journal(engine)
        print(f"Control tables ready ({time.monotonic() - started:.0f}s)")
        if not args.dry_run:
            recover_journal(engine, client)
        print(f"Journal checked ({time.monotonic() - started:.0f}s)")
        targets = _targets(engine, [s for s in args.schemas.split(",") if s], args.tables)

        registration_failures = 0
        for schema, table in targets:
            if out_of_budget():
                print(f"Time budget used up registering folders; {schema}.{table} onwards wait")
                break
            try:
                register_day_folders(engine, client, args.prefix, schema, table)
            except Exception as e:
                # One table's slow listing must not stop the others; it is retried next run.
                registration_failures += 1
                print(f"FAILED registering {schema}.{table}: {type(e).__name__}: {e}")
        print(f"Folders registered ({time.monotonic() - started:.0f}s)")
        queues = [candidate_days(engine, s, t, cfg) for s, t in targets]
        pending_folders = sum(len(q) for q in queues)
        print(
            f"Compaction run {run_ts}: {len(targets)} tables, {pending_folders:,} candidate day "
            f"folders, min_parts={cfg.min_parts}, fold_after={cfg.fold_after}, "
            f"grace_hours={cfg.grace_hours}, max_seconds={args.max_seconds}, "
            f"workers={args.workers}, dry_run={args.dry_run}"
        )

        totals = {"done": 0, "removed": 0, "created": 0, "rows": 0, "failures": 0, "folds": 0}

        def work(schema: str, table: str, day: str):
            started_folder = time.monotonic()
            try:
                r = compact_folder(
                    engine, client, args.prefix, schema, table, day, run_ts, cfg, args.dry_run
                )
            except Exception as e:
                return schema, table, day, None, f"{type(e).__name__}: {e}", 0.0
            return schema, table, day, r, None, time.monotonic() - started_folder

        def record(done_futures) -> None:
            for future in done_futures:
                schema, table, day, r, error, took = future.result()
                if error:
                    totals["failures"] += 1
                    print(f"FAILED {schema}.{table}/{day}: {error}")
                    continue
                totals["done"] += 1
                totals["removed"] += r["removed"]
                totals["created"] += r["created"]
                totals["rows"] += r["rows"]
                totals["folds"] += int(r["folded"])
                if r["removed"]:
                    print(
                        f"{schema}.{table}/{day}: {r['removed']} objects -> {r['created']}"
                        f"{' (folded)' if r['folded'] else ''} ({r['rows']:,} rows, {took:.1f}s)"
                    )

        stopped = False
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            in_flight: set = set()
            for batch in zip_longest(*queues):
                for (schema, table), day in zip(targets, batch, strict=True):
                    if day is None:
                        continue
                    if out_of_budget():
                        stopped = True
                        break
                    while len(in_flight) >= args.workers:
                        finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                        record(finished)
                    in_flight.add(pool.submit(work, schema, table, day))
                if stopped:
                    break
            record(wait(in_flight)[0])

        pruned = 0 if args.dry_run else prune_runs(engine, args.prune_runs_days)
        print(
            f"Done in {time.monotonic() - started:.0f}s: {totals['done']:,}/{pending_folders:,} "
            f"folders visited, {totals['removed']:,} objects merged into {totals['created']:,} "
            f"files ({totals['folds']} folds, {totals['rows']:,} rows), "
            f"{totals['failures']} failures, {pruned:,} old manifests pruned"
            + (", stopped by --max-seconds" if stopped else "")
        )
        if totals["failures"] or registration_failures:
            sys.exit(1)
    finally:
        if lock_conn is not None:
            lock_conn.close()
        engine.dispose()


if __name__ == "__main__":
    main()
