# Incremental ingestion (watermark-based), a stepping stone before CDC

`dbapi/api/seeder/ingestions/` (one script per OLTP schema, landing raw Parquet into MinIO's
`landing` bucket) used to re-read every table **in full, on every run** — no `WHERE` clause at
all. Fine when the tables were small; not fine once `marketing.dailyperformance` reached 18.7M
rows / 15GB. Written 2026-09-04.

## The problem

Two symptoms, same root cause:

1. **`nexomart_ingestion_dag`'s `ingest_marketing` task ran 30+ minutes and kept growing in
   memory** — a manual run triggered 2026-09-04 10:37 UTC was still running, RSS still
   climbing, 34 minutes in, re-scanning all 18.7M rows of `marketing.dailyperformance` from
   scratch (again — same table, same rows, every single run).
2. **`ingest_product` was getting OOM-killed** (`dmesg`: `Memory cgroup out of memory: Killed
   process ... python3`, repeated). Looked at first like a product-specific bug, but the
   isolated cause was two things stacking, not a code defect in the product script itself:
   - `airflow_worker` has a 6GB container memory limit, and at the time of the failures it was
     already running 3+ concurrent real tasks (the marketing ingestion above, a sales
     ingestion, a marketing seeder), together already using 3.3-3.4GB before product's task
     even started.
   - `product.productembedding` (100,000 rows × 2 pgvector `vector` columns) is genuinely the
     heaviest table in that schema by memory footprint per row — not huge on its own, but
     enough to be the tipping point on top of everything else already running.

Both are consequences of the same design gap: every run re-extracts 100% of every table,
regardless of how little (or nothing) actually changed since the last run. As tables grow —
and they will, `nexomart_seeder_dag` runs every 15 minutes now (see the schedule-interval
change the same day) — the cost of "everything, every time" only gets worse, and it competes
for the same 6GB container with whatever else Airflow happens to be running concurrently.

## What was done: watermark-based incremental extraction

Every model already carries `created_at`/`updated_at` via `AuditMixin` — `created_at` is set on
insert, `updated_at` only on a later update (`onupdate=func.now()`, nullable, `NULL` until then).
That's enough to filter for "rows new or changed since last time" without any new
infrastructure:

```sql
SELECT * FROM "<schema>"."<table>" WHERE COALESCE(updated_at, created_at) > :watermark
```

**Watermark storage** — a small, self-bootstrapping control table (created on first use by any
ingestion script, no separate migration to remember to run):

```sql
CREATE SCHEMA IF NOT EXISTS ingestion_control;
CREATE TABLE IF NOT EXISTS ingestion_control.watermarks (
    schema_name text NOT NULL,
    table_name text NOT NULL,
    last_watermark timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, table_name)
);
```

**Ordering, to avoid ever losing a row**: `ingest_table()` captures `extract_start =
datetime.now(timezone.utc)` *before* running the query, and only writes it as the new
watermark *after* the table's extraction (streaming read + all Parquet parts uploaded)
finishes without raising. A row written to Postgres while the extraction is mid-flight is
outside the query's MVCC snapshot (taken at query start under the default `READ COMMITTED`
isolation), so it's simply not read this run — but its `created_at`/`updated_at` will be
`>= extract_start`, so it's guaranteed to be `> ` the new watermark and gets picked up
automatically on the *next* run. No gap, no double-counting, no manual bookkeeping.

**A table with no `created_at`/`updated_at` never breaks the filter.** `_has_audit_columns()`
checks `information_schema.columns` per table and falls back to the old full-scan behavior
for any table missing either column — as of this writing, only `product.productcategory`
predates `AuditMixin` and hits this path.

Code: `dbapi/api/seeder/ingestions/ingestion_common.py` — `get_watermark()`, `set_watermark()`,
`_ensure_watermark_table()`, `_has_audit_columns()`, and the changed `ingest_table()`.

## Landing path, unaffected by this change

Still `nexomart/<schema>/<table>/<YYYY-MM-DD>/part-<run_ts>-NNNNN.parquet` (day-partitioned,
per-run files, additive — see the git history of `ingestion_common.py`'s module docstring for
that change, made the same day). Incremental extraction only changes *which rows* a run reads;
it doesn't change where they land. A run that picks up zero new/changed rows for a table simply
writes zero parts for it that day — no empty files.

## Measured, live, same day (2026-09-04)

`humanresources` (small schema, safe to iterate on quickly):

| Run | What changed in Postgres first | Rows landed |
|---|---|---|
| 1st ever run | — (first run ever, watermark = epoch) | 19,055 (full backfill: 16 + 18,949 + 90) |
| 2nd run, immediately after | nothing | **0** |
| 3rd run, after seeding 5 new employees | +5 rows in `humanresources.employee` | **5** |

`product` (the schema that had been OOM-killing): after the watermark change, a full run
landed successfully — `product.product`: 0 (already covered by an earlier, partially-completed
attempt's watermark), `productcategory`: 19 (no-audit-column fallback path), `productembedding`:
100,000 (first-ever backfill for that table), `productmodel`: 19. No crash. Every run after this
one only reads what actually changed — the 100k-row `productembedding` backfill cost is paid
once, not every 15 minutes.

## What this is *not* — and the actual plan for CDC

This is deliberately a stepping stone, not a replacement for real CDC:

- **Still polling.** Every run pays the cost of scanning for the *changed* rows — cheap, but
  not free — and there's an unavoidable detection lag equal to the DAG's schedule interval
  (currently 15 minutes).
  ~~The filter isn't index-optimal yet~~ — fixed later the same day: `COALESCE(updated_at,
  created_at)` forced a Seq Scan even with `created_at` indexed (function-wrapped columns
  aren't sargable against a plain index), so each of the 5 partitioned tables got a matching
  expression index. Details, including why `CREATE INDEX CONCURRENTLY` had to be built
  per-partition and attached (it doesn't work directly on a partitioned table): see
  `configs/postgresql/README.md`.
- **Can't see a delete.** A row `UPDATE`d and then hard-`DELETE`d between two runs is invisible
  to this mechanism — the watermark filter only ever sees rows that still exist. Nothing in
  this schema currently does hard deletes (`is_active` soft-delete flag from `AuditMixin`
  covers the seeded chaos scenarios), so this hasn't mattered yet, but it would need Postgres's
  own replication/WAL (real CDC) to handle correctly if that changes.
- **No schema-change awareness.** A column added/dropped/renamed on the Postgres side isn't
  detected or reconciled — same as before this change, not a regression, just still open.

When CDC (Debezium + Postgres logical replication is the natural fit here, given the stack) is
implemented, it replaces the polling watermark with continuous WAL streaming — but the
downstream shape (day-partitioned Parquet in the `landing` bucket) doesn't need to change for
that migration; only *how* rows arrive at `ingest_table()`'s door changes, not what it does with
them once they're there.

## Two more real bugs, found chasing a "stuck" `ingest_sales` (later 2026-09-04)

Running a full backfill for real (all 7 schemas, watermarks all at epoch) surfaced two
completely separate bugs that the small-scale testing above hadn't hit. Both looked identical
from the outside — a task showing "running" for 40+ minutes with zero new Parquet parts — but
had different causes and different fixes. Both containers only get their `pip install`s back on
rebuild, not persisted, so debugging tools mentioned below (`py-spy`) need reinstalling per
container recreation.

### Bug 1 — MinIO client had no timeout

`configs/api/minio.py` built its `Minio(...)` client with no `http_client` argument, so the SDK
fell back to a default `urllib3.PoolManager` with no read/connect timeout. A stalled TCP
connection during `put_object()` (a real, if rare, occurrence on this stack — see the WSL2/
Docker Desktop networking issues documented elsewhere this session) blocks the calling thread
**forever**, no exception ever raised — which also means `_write_parquet_with_retry()`'s
retry-with-backoff loop never even gets a chance to run, since it only fires on a caught
exception.

Fixed by passing an explicit `urllib3.PoolManager(timeout=urllib3.Timeout(connect=10, read=60),
retries=urllib3.Retry(total=0))` as the client's `http_client`, and broadening
`write_on_minio()`'s `except` clause from just `S3Error` to also catch `urllib3.exceptions.
HTTPError` and `OSError` — the connection-level failures the timeout now actually raises instead
of hanging on. This is a real, worthwhile fix (confirmed via a live smoke-test write) but turned
out **not** to be the cause of the `ingest_sales` stall specifically — that was bug 2.

### Bug 2 — the actual cause: psycopg2 decode → re-encode round-trip

Diagnosing bug 2 needed a live stack trace of the stuck process. `py-spy` needs `ptrace`, which
Docker drops by default — `airflow/docker-compose.yaml`'s `airflow-worker` service now has
`cap_add: [SYS_PTRACE]` for exactly this. (`docker exec -u root <container> py-spy dump --pid
<pid>` from there.)

Three samples of the stuck `sales_ingestion.py`, a few seconds apart, showed **varying** frames
— `json/encoder.py`, `cur.fetchmany`, `json/decoder.py` — which first ruled out an actual hang
(a genuine deadlock shows the *same* frame every time; this was doing real, if slow, work). The
`json.decoder`/`json.loads` frame was the tell: nothing in `ingestion_common.py` calls
`json.loads` — that's psycopg2's own JSONB adapter, which auto-decodes every `jsonb` column into
a Python `dict`/`list` on fetch. `_normalize_arrow_value()` then immediately calls `json.dumps()`
to turn it right back into a string (needed because a JSONB column can legitimately mix real
dicts with chaos-injected malformed scalars, and pyarrow can't write a column that isn't one
consistent type). Postgres already sent the value as text; decoding it into a dict just to
re-serialize it back is a pure round-trip, no-op in terms of the actual output, and it's
per-value pure-Python work (no C-level speedup) — on a 100k-row chunk with a lot of genuine
dict/list content in `order_items`/`payment_info`, that's up to 200,000 individual
encode+decode calls for one chunk.

Fixed with two lines in `ingest_schema()`, right after `raw_conn = engine.raw_connection()`:

```python
psycopg2.extras.register_default_jsonb(raw_conn.dbapi_connection, loads=lambda raw: raw)
psycopg2.extras.register_default_json(raw_conn.dbapi_connection, loads=lambda raw: raw)
```

A no-op `loads` makes psycopg2 hand back the raw text Postgres sent instead of decoding it —
skipping both the decode *and* the now-unnecessary re-encode. (`raw_conn.dbapi_connection`, not
`raw_conn` itself — `engine.raw_connection()` returns a SQLAlchemy `_ConnectionFairy` wrapper;
`register_default_jsonb` needs the real `psycopg2.extensions.connection` underneath.) Verified
the resulting Parquet still has valid, correctly-formatted JSON strings (round-tripped a
non-trivial value with nested list/float content through `json.loads` after reading it back).

## Postmortem: why `ingest_sales` took 4 tries to ever finish

Even after both fixes, `ingest_sales` kept stalling on the same partition
(`sales.salesorder_2026_08`) for 10-40+ minutes at a time, on tries 1 through 4. `py-spy`
showed genuine, varying work each time (not the bugs above) — the actual cause turned out to be
external to the code entirely: **`sales.salesorder` grew from 12.6M to 31.9M rows *during* this
debugging session** — the seeder DAG kept running every 15 minutes for hours while this was
being chased, and `sales` is the highest-volume schema in the whole platform. Because no attempt
had yet completed successfully, the watermark was still at epoch on every retry — so every retry
re-raced a full backfill against a partition that had only gotten bigger since the last attempt
(`salesorder_2026_08` alone reached 28.6M rows / 40GB). Not a bug: a full backfill against a
table under sustained concurrent write load will always be slower than the same backfill against
a static table, and with the watermark never landing, every retry paid that cost again from
scratch. Once it finally succeeded (try 4, 26m42s, largely thanks to bug 2's fix removing the
worst of the per-row overhead), the watermark for every table in the schema was set, and it goes
back to being a normal incremental read from here — the same pattern `humanresources`/`product`
already demonstrated above.

## The seeder no longer auto-triggers ingestion

`nexomart_seeder_dag.py` used to end every run with a `TriggerDagRunOperator` (`trigger_ingestion`,
`wait_for_completion=False`) that fired `nexomart_ingestion_dag` unconditionally, every 15
minutes, regardless of whether the *previous* ingestion run had finished. Ingestion runs
regularly took far longer than 15 minutes (see the postmortem above), so triggers stacked up as
queued `nexomart_ingestion_dag` runs behind each other — two were found queued and cleared
(`DELETE FROM dag_run WHERE dag_id='nexomart_ingestion_dag' AND state='queued'`) after removing
the trigger. Removed 2026-09-04. `nexomart_ingestion_dag` is now fully decoupled: trigger it
manually, or put it on its own schedule sized to how long it actually takes — not to the
seeder's cadence.

## Operational notes

- **Force a full re-backfill for one table**: `DELETE FROM ingestion_control.watermarks WHERE
  schema_name = '<schema>' AND table_name = '<table>';` — next run for that table falls back to
  the epoch default and re-reads everything.
- **Check what's been ingested and when**: `SELECT * FROM ingestion_control.watermarks ORDER BY
  updated_at DESC;` — `updated_at` here is when the watermark itself last advanced, i.e. the
  last time that table had a successful (possibly zero-row) ingestion run.
- **A watermark only advances on success.** A crashed/OOM-killed run (like the product
  incidents above) leaves the watermark exactly where it was before that run started — the next
  run picks up from the same point, nothing is silently skipped because of a crash.
- **Getting a live stack trace of a task that looks stuck**: `airflow-worker` has `cap_add:
  [SYS_PTRACE]` (`airflow/docker-compose.yaml`) for exactly this. Install `py-spy` in the venv
  the seeder/ingestion scripts actually run in (lost on every container recreation, not
  persisted — reinstall each time): `docker exec airflow_worker sh -c "PYTHONPATH=/dockervol:
  /dockervol/dbapi uv run --project /dockervol pip install py-spy -q"`, find the PID (`docker
  exec airflow_worker ps aux | grep <script>.py` — the `.../.venv/bin/python3` line, not the
  `uv run` wrapper), then `docker exec -u root airflow_worker /home/astro/.local/bin/py-spy dump
  --pid <pid>`. Take 2-3 samples a few seconds apart before concluding anything: a **fixed**
  frame across samples means a genuine hang; **varying** frames mean it's doing real (if slow)
  work — this distinction is what separated the two real bugs above from the postmortem's
  "just a big table" non-bug.
