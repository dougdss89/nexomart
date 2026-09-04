"""
Shared plumbing for the nexomart landing-zone ingestion scripts (dbapi/api/seeder/ingestions/).

One script per OLTP schema (see CLAUDE.md's 7 domains) reads every table currently in that
Postgres schema and lands it as raw Parquet in MinIO's `landing` bucket, under
`nexomart/<schema>/<table>/<YYYY-MM-DD>/part-<run_ts>-NNNNN.parquet` — the Bronze entry point
described in docs/nexomart_requisitos.md (FR-03/FR-04). Triggered right after the seeders
finish, see the `ingestion` TaskGroup in airflow/dags/nexomart/seeder/nexomart_seeder_dag.py.

Partitioned by day, not by run: every ingestion run that lands within the same UTC day appends
its parts into the same `<YYYY-MM-DD>/` folder instead of each run getting its own partition
(the previous `ingestion_ts=<run>` scheme, one partition per run — too fine-grained once the
seeder DAG moved to a 15min cadence, ~96 tiny partitions/day). The full per-run timestamp
(`run_ts`) still goes into the filename itself so concurrent/same-day runs never collide or
overwrite each other's parts — this is additive, not a replace.

Reads use a sync psycopg2 engine (pandas has no async support) built from the same
PGUSER/PGPWD/POSTGREIP/PGPORT/PGDATABASENAME settings the rest of dbapi uses for asyncpg —
never a second, hardcoded connection string (CLAUDE.md's "Zero Hardcode" rule).

Tables are discovered from `pg_tables` rather than hardcoded, so a schema migration that adds
a table doesn't silently leave it out of the landing zone. Reads stream via a **named (server-side)
psycopg2 cursor**, one Parquet part uploaded per fetched batch. `pandas.read_sql_query(...,
chunksize=...)` looks like it streams but doesn't: a plain psycopg2 cursor buffers the *entire*
resultset client-side on `execute()` regardless of `chunksize` (that only affects how pandas
slices the already-fully-fetched rows afterwards) — confirmed the hard way: `sales_ingestion.py`
against `sales.salesorder` (8.7M rows, JSONB) OOM-killed the airflow-worker container (6GB
limit) using that approach. A named cursor (`connection.cursor(name=...)`) makes Postgres itself
paginate the result over the wire in `itersize`-sized batches, so memory stays bounded by
`chunksize` regardless of table size (needed here — purchasing.purchaseorderaudit alone is ~5.9M
rows, and FR-01 targets up to 50M).

Incremental extraction (2026-09-04): every run used to re-read the *entire* table, every time —
harmless at the row counts this had when the ingestion DAG was young, but by the time
marketing.dailyperformance reached 18.7M rows / 15GB a single re-scan ran 30+ minutes and grew
memory usage the whole time, starving other concurrently-scheduled tasks in the same
worker container of RAM (observed OOM-killing an unrelated product ingestion task that
happened to run at the same time — not a product-specific bug, just the first victim of the
shared container's memory ceiling). `ingest_table()` now filters on
`COALESCE(updated_at, created_at) > watermark`, using the same AuditMixin columns every table
already carries, and advances a per-(schema, table) watermark (stored in
`ingestion_control.watermarks`, self-bootstrapped on first use — see `_ensure_watermark_table`)
to the extraction's own start time once a run succeeds. This is deliberately a stepping stone,
not a replacement for real CDC: it's poll-based (still costs a scan of the *changed* rows every
run, just not the unchanged ones) and has no way to see a row that was updated and then deleted
between two runs. When CDC (e.g. Debezium/logical replication) lands, it replaces this polling
watermark with continuous WAL streaming — the day-partitioned Parquet landing structure this
writes to doesn't need to change either way. See docs/INGESTION_INCREMENTAL.md.

A couple of tables predate AuditMixin and have neither column (`product.productcategory`, as of
this writing) — `_has_audit_columns()` detects that per-table and falls back to a full scan for
just that table rather than crashing or silently skipping it.
"""

import io
import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import psycopg2.extras
from sqlalchemy import create_engine, text

from api.config import settings, write_on_minio

LANDING_BUCKET = "landing"
LANDING_PREFIX = "nexomart"
DEFAULT_CHUNKSIZE = 100_000
MINIO_WRITE_MAX_ATTEMPTS = 4
MINIO_WRITE_BACKOFF_SECONDS = 2  # doubles each retry: 2s, 4s, 8s

WATERMARK_SCHEMA = "ingestion_control"
WATERMARK_TABLE = "watermarks"
# Default watermark for a (schema, table) that has never been ingested — old enough that
# `> EPOCH` matches every existing row, giving a full backfill on the first run.
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _sync_database_url() -> str:
    """psycopg2 counterpart of settings.DATABASE_URL (which is asyncpg-only, pandas can't use it)."""
    return (
        f"postgresql+psycopg2://{settings.PGUSER}:{settings.PGPWD}"
        f"@{settings.POSTGREIP}:{settings.PGPORT}/{settings.PGDATABASENAME}"
    )


def list_schema_tables(engine, schema: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = :schema ORDER BY tablename"),
            {"schema": schema},
        )
        return [r[0] for r in rows]


def _ensure_watermark_table(engine) -> None:
    """Self-bootstraps the control table — no separate migration step to remember, and it's a
    no-op after the first call anywhere (any schema's ingestion script can trigger it)."""
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{WATERMARK_SCHEMA}"'))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{WATERMARK_SCHEMA}"."{WATERMARK_TABLE}" (
                schema_name text NOT NULL,
                table_name text NOT NULL,
                last_watermark timestamptz NOT NULL,
                updated_at timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (schema_name, table_name)
            )
        '''))


def _has_audit_columns(engine, schema: str, table: str) -> bool:
    """AuditMixin gives every model `created_at`/`updated_at`, but a couple of tables predate
    it (see module docstring) — those fall back to a full scan instead of erroring on a filter
    against columns that don't exist."""
    with engine.connect() as conn:
        count = conn.execute(
            text(
                "SELECT count(*) FROM information_schema.columns "
                "WHERE table_schema = :schema AND table_name = :table "
                "AND column_name IN ('created_at', 'updated_at')"
            ),
            {"schema": schema, "table": table},
        ).scalar()
        return count == 2


def get_watermark(engine, schema: str, table: str) -> datetime:
    _ensure_watermark_table(engine)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                f'SELECT last_watermark FROM "{WATERMARK_SCHEMA}"."{WATERMARK_TABLE}" '
                "WHERE schema_name = :schema AND table_name = :table"
            ),
            {"schema": schema, "table": table},
        ).first()
    return row[0] if row else EPOCH


def set_watermark(engine, schema: str, table: str, ts: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(f'''
                INSERT INTO "{WATERMARK_SCHEMA}"."{WATERMARK_TABLE}" (schema_name, table_name, last_watermark)
                VALUES (:schema, :table, :ts)
                ON CONFLICT (schema_name, table_name)
                DO UPDATE SET last_watermark = EXCLUDED.last_watermark, updated_at = now()
            '''),
            {"schema": schema, "table": table, "ts": ts},
        )


def _normalize_arrow_value(value):
    """psycopg2 hands back a few Postgres types as Python objects pyarrow can't write as-is:

    - `uuid` columns (e.g. AuditMixin's row_guid) come back as `uuid.UUID` instances —
      pyarrow can't infer an Arrow type for those (`did not recognize Python value type`).
    - `jsonb` columns come back pre-decoded as `dict`/`list`.

    Both get stringified. This runs per-value (not per-column, e.g. by peeking at the first
    non-null value) because a jsonb column can legitimately mix real dicts with malformed
    scalars in the same column — CLAUDE.md's chaos engineering rules deliberately inject dirty
    data (e.g. a plain string instead of a JSON object) into these columns, which is exactly the
    heterogeneity a landing zone needs to preserve as-is rather than choke on
    (`ArrowInvalid: Could not convert 'Sem Garantia' with type str: tried to convert to int64`,
    hit importing product.productdetails). Everything else (including a value already `None`)
    passes through unchanged.
    """
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value


def _normalize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(_normalize_arrow_value)
    return df


def _write_parquet_with_retry(object_name: str, data: bytes) -> None:
    """Uploads one Parquet part to MinIO, retrying transient write failures with backoff.

    Root cause (found running all 7 schemas concurrently for real via nexomart_ingestion_dag,
    2026-08-28): MinIO itself logged `IncompleteBody` errors on its own internal bucket-usage
    scanner files during the run, alongside a `put_object` failing with the same error on a
    `marketing.dailyperformance` part — disk I/O contention on WSL2's virtualized disk under
    several concurrent multi-hundred-MB uploads, the same root cause already documented for the
    seeders in docs/SEEDER_MICROBATCHING.md. Only ever 1 part out of dozens failed per occurrence
    (confirmed twice), so this is a transient per-request blip, not a systemic problem worth
    giving up parallelism for (unlike the seeders' fix, which serializes 3 specific heavy tasks —
    not appropriate here since any of the 7 schemas' writes could be the one that blips).
    """
    for attempt in range(1, MINIO_WRITE_MAX_ATTEMPTS + 1):
        if write_on_minio(LANDING_BUCKET, object_name, data):
            return
        if attempt == MINIO_WRITE_MAX_ATTEMPTS:
            raise RuntimeError(f"Failed to write {object_name} to MinIO after {attempt} attempts")
        wait = MINIO_WRITE_BACKOFF_SECONDS * (2 ** (attempt - 1))
        print(f"   ⚠️  MinIO write failed for {object_name} (attempt {attempt}/{MINIO_WRITE_MAX_ATTEMPTS}), retrying in {wait}s...")
        time.sleep(wait)


def ingest_table(
    raw_conn, engine, schema: str, table: str, run_date: str, run_ts: str, chunksize: int = DEFAULT_CHUNKSIZE
) -> int:
    """Streams `schema.table` into MinIO landing as one Parquet part per fetched batch, via a
    named psycopg2 cursor so Postgres paginates server-side instead of the client buffering the
    whole resultset. Returns row count.

    Parts land under `<run_date>/`, appending alongside any other run's parts from the same
    day — `run_ts` (unique per run) is folded into the filename instead of the partition path
    so same-day runs can never collide on `part-NNNNN.parquet` and overwrite each other.

    Incremental by default: only rows with `COALESCE(updated_at, created_at) > watermark` are
    read, where `watermark` is this table's last successful extraction (EPOCH — matching
    everything — on the first run). The new watermark is captured *before* the query runs and
    only persisted after a successful extraction, so a row written mid-run is never lost (it
    just won't be picked up until next run, same as a row written a second later would be).
    Tables without both audit columns (see `_has_audit_columns`) always get a full scan.
    """
    incremental = _has_audit_columns(engine, schema, table)
    watermark = get_watermark(engine, schema, table) if incremental else None
    extract_start = datetime.now(timezone.utc)

    total_rows = 0
    with raw_conn.cursor(name=f"ingest_{schema}_{table}") as cur:
        cur.itersize = chunksize
        if incremental:
            cur.execute(
                f'SELECT * FROM "{schema}"."{table}" WHERE COALESCE(updated_at, created_at) > %s',
                (watermark,),
            )
        else:
            cur.execute(f'SELECT * FROM "{schema}"."{table}"')
        columns = None  # a named cursor's .description is only populated after the first fetch
        part = 0
        while True:
            rows = cur.fetchmany(chunksize)
            if not rows:
                break
            if columns is None:
                columns = [d.name for d in cur.description]
            chunk = _normalize_object_columns(pd.DataFrame(rows, columns=columns))
            buffer = io.BytesIO()
            chunk.to_parquet(buffer, engine="pyarrow", index=False)
            object_name = f"{LANDING_PREFIX}/{schema}/{table}/{run_date}/part-{run_ts}-{part:05d}.parquet"
            _write_parquet_with_retry(object_name, buffer.getvalue())
            total_rows += len(chunk)
            print(f"   part-{part:05d}: {len(chunk):,} rows -> s3://{LANDING_BUCKET}/{object_name}")
            part += 1
    raw_conn.commit()  # closes the read-only transaction the named cursor ran in

    if incremental:
        set_watermark(engine, schema, table, extract_start)

    return total_rows


def ingest_schema(schema: str, chunksize: int = DEFAULT_CHUNKSIZE) -> None:
    """Lands every table currently in `schema` into the nexomart landing zone."""
    engine = create_engine(_sync_database_url())
    raw_conn = engine.raw_connection()
    # psycopg2 auto-decodes jsonb/json columns into dict/list on fetch (via its own json.loads),
    # which _normalize_arrow_value then immediately re-encodes back to a string with json.dumps
    # (needed because a jsonb column can legitimately mix real dicts with chaos-injected
    # malformed scalars — pyarrow can't write a column that isn't a single consistent type).
    # That's a pure round-trip: Postgres already sends the value as text; decoding it into a
    # dict just to re-serialize it back is wasted work, and it's pure-Python per-value work, no
    # C-level speedup — confirmed live 2026-09-04 via py-spy on a stuck-looking (actually just
    # slow) ingest_sales run: alternating json.decoder/json.encoder frames on a JSONB-heavy
    # 100k-row chunk. Registering a no-op loader makes both columns come back as the raw text
    # Postgres already sent, skipping the decode (and therefore the matching re-encode) entirely.
    psycopg2.extras.register_default_jsonb(raw_conn.dbapi_connection, loads=lambda raw: raw)
    psycopg2.extras.register_default_json(raw_conn.dbapi_connection, loads=lambda raw: raw)
    try:
        now = datetime.now(timezone.utc)
        run_date = now.strftime("%Y-%m-%d")
        run_ts = now.strftime("%Y%m%dT%H%M%SZ")
        tables = list_schema_tables(engine, schema)
        print(
            f"📦 Ingesting schema '{schema}' -> s3://{LANDING_BUCKET}/{LANDING_PREFIX}/{schema}/ "
            f"({len(tables)} tables, ingestion_date={run_date}, run_ts={run_ts})"
        )

        total_rows = 0
        for table in tables:
            rows = ingest_table(raw_conn, engine, schema, table, run_date, run_ts, chunksize=chunksize)
            total_rows += rows
            print(f"✅ {schema}.{table}: {rows:,} new/changed rows landed")

        print(f"🏁 Schema '{schema}' done: {total_rows:,} total rows across {len(tables)} tables")
    except Exception as e:
        print(f"❌ Ingestion of schema '{schema}' failed: {e}")
        raise
    finally:
        raw_conn.close()
        engine.dispose()
