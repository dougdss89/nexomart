# Seeder micro-batching for a 10-15 minute DAG cadence

`nexomart_seeder_dag` was originally scheduled daily (`schedule="0 2 * * *"`). The plan is to
move it to every 10-15 minutes instead — this document is why three specific things had to
change first, and what was actually done. Written 2026-08-17.

## The problem

The user observed, testing the DAG manually, that a handful of tasks took much longer than
the rest and asked for a slowdown analysis before committing to a tight cadence. Real
measurements across 3 manual Airflow runs on 2026-08-17 (`task_instance` durations, not
estimates):

| Task | Avg | Max |
|---|---|---|
| `derived.person_customer_email` | 12.7 min | 32.6 min |
| `derived.purchase_detail` | 11.6 min | 33.6 min |
| `derived.purchase_audit` | 8.8 min | 22.0 min |
| `dimensions.person_customer` | 8.1 min | 17.0 min |
| `facts.sales_order` | 7.6 min | 21.3 min |
| everything else (25 tasks) | <2.6 min | <5.6 min |

A single task already exceeding a 15-minute SLA in the worst case is disqualifying on its own.
Two causes, stacked:

1. **Volumes were sized for "a day," not "10-15 minutes."** `configs/seeders/config.py`'s
   ranges went up to 1,000,000 rows per run for `sales`/`purchasing`/`warehouse`/`person` —
   reasonable for a daily cron, wildly oversized for ~100 runs/day.
2. **Three "fan-out" seeders and one FK-sampling seeder scaled with total accumulated
   history, not with the work actually needed that run.** This is the deeper bug — fixing
   volume alone (item 1) wouldn't have prevented these four from eventually blowing the SLA
   again after enough 10-minute runs accumulated more history.

Importantly: the intent behind fetching real IDs from the DB before generating rows (rather
than fabricating them) is correct and deliberate — it's standing in for what an API in a real
e-commerce platform would validate against a real request. Nothing below changes that
guarantee. Every ID any seeder uses after this fix is still a real, existing row in the
database — only *how* the query finds those real IDs changed.

## Fix 1 — fan-out seeders: anti-join, then high-water-mark (the anti-join alone wasn't enough)

`person_customer_email.py`, `purchase_purchaseorderdetail.py`, and
`purchase_purchaseorderaudit.py` don't draw an independent row count — they generate exactly
one derived row (or a few) per *pending* parent row (customer without an email, header
without detail/audit lines). The original code found "pending" by loading **every** ID from
both the parent table and the already-processed child table into Python and diffing them
there with `if id not in already_processed`. Cost scaled with total accumulated rows in both
tables — after only 3-4 test runs, `person.customer` had ~2.1M rows and
`purchasing.purchaseorderheader` ~1.4M; this is exactly what produced the 8-32 minute numbers
above, and it would only get worse the longer the DAG ran on any cadence.

**First attempt**: replace the Python diff with a SQL anti-join (`LEFT JOIN parent
LEFT JOIN child ON ... WHERE child.fk IS NULL LIMIT n`), backed by a new index on the child's
FK column (`ix_customer_email_customer_id`, `ix_purchaseorderdetail_header_id`,
`ix_purchaseorderaudit_header_id`). Correct, but measured **still slow** (3m+) against the
live ~2M-row tables: Postgres has no index that identifies *which* rows are unmatched, so a
Nested Loop Anti Join still walks the parent table roughly in physical/insertion order until
it accumulates `n` misses — i.e. it still scans through most of the *matched* rows first if
the unmatched ones aren't clustered near the start of the table (they're not; they're the
newest rows, at the end).

**Actual fix**: a high-water-mark range scan instead of a join at all.

- `person_customer_email.py`: `customer_id` is a plain autoincrement integer, so "pending" =
  `customer.customer_id > MAX(customer_email.customer_id)`. A pure indexed range scan on the
  customer PK, no join.
- `purchase_purchaseorderdetail.py` / `purchase_purchaseorderaudit.py`: `purchaseorderheader_id`
  is a random `PO-XXXXXXXXX` business ID (see CLAUDE.md's transactional ID prefixes) — **not**
  sequential, so it can't be used as a watermark (a later-created header can sort lower than
  an earlier one). Used `created_at` instead (indexed — see "AuditMixin gap" below): pending =
  `header.created_at > MAX(detail_or_audit.created_at)`.

This is correct as long as (a) a run either fully commits its fan-out batch or rolls back
entirely on error — true for all three (`except: rollback(); raise`) — and (b) the fan-out
task always runs strictly after its parent task within the same DAG execution — true per the
DAG's `facts`/`dimensions` → `derived` TaskGroup ordering. Both were already true; nothing in
the DAG structure had to change.

**Measured after the fix** (`--no-chaos`, live ~2-3.6M-row tables, same environment as the
original measurements):

| Task | Before (avg / max) | After |
|---|---|---|
| `person_customer_email.py` | 12.7 min / 32.6 min | 2m40s (11,960 rows) — SELECT itself: ~2s |
| `purchase_purchaseorderaudit.py` | 8.8 min / 22.0 min | 3m41s (23,015 rows) — SELECT itself: <1s |
| `purchase_purchaseorderdetail.py` | 11.6 min / 33.6 min | 10m44s (24,960 rows) — SELECT itself: <1s, see caveat below |

The SELECT/watermark side is fixed in all three cases (sub-2-second, confirmed by watching
`pg_stat_activity` transition to the INSERT/COPY phase almost immediately). `purchase_purchaseorderdetail.py`'s
total is still high because the *insert* side is now the bottleneck: `purchaseorderdetail` had
accumulated ~3.6M rows from today's repeated manual testing (not representative of real
10-15min-cadence steady state) and — unlike the 5 tables in `configs/postgresql/migrations/`
— was never partitioned, so its indexes don't get the "smaller working set per partition"
benefit those tables now have. Worth revisiting if it's still slow once the DB reflects real
steady-state volume instead of today's test data; not fixed here (out of scope — would mean
partitioning a 6th table, a bigger structural change than the 3 items agreed on).

Each fan-out is also now bounded per run by `LIMIT <parent table's max_count>` (from
`configs/seeders/config.py`) — in steady state this always covers a run's new parents; if a
backlog ever exists (e.g. after a paused period), it's drained gradually across runs instead
of one run trying to process all of it at once.

## Fix 2 — FK-sampling seeders: server-side random sample, not the whole column

`sales_order.py` loaded `select(CustomerModel.customer_id)).scalars().all()` — the entire
`person.customer` ID column, currently ~2.1M values — just to `random.choice()` a customer
per new order. Same class of bug as Fix 1 (cost scales with accumulated history, not with
rows generated this run), different shape (no "pending" concept here, it's pure sampling).

**First attempt**: a new `sample_fk_ids()` helper using `ORDER BY random() LIMIT n`. This is a
well-known Postgres trap, caught by actually testing it (not just reasoning about it): Postgres
has to compute a random sort key for **every** row and sort the whole table before taking the
top `n` — measured still stuck at 2min+ against ~2.1M customers, no better than the original
bug. `ORDER BY random()` bounds what comes back to Python, but not what Postgres has to touch
to get there.

**Actual fix**: `TABLESAMPLE SYSTEM (pct)` — picks random *blocks* (pages) and returns whatever
rows are in them, touching only ~`pct`% of the table's pages regardless of table size, no sort.
Measured 52ms for 5,000 rows out of ~2.1M (vs. 2min+ stuck). `sample_fk_ids()` falls back to a
plain bounded `LIMIT` fetch if the sample comes back empty (small/sparse table — exactly the
case where a full read isn't a scaling concern anyway). Applied to `sales_order.py`'s
`customer_ids` only — its other FK pools (channels, offers, sellers, products) stay
small/bounded and don't need it. Measured end-to-end: `sales_order.py` went from avg 7.6min /
max 21.3min to **1m5s** for 2,332 rows.

`dimensions.person_customer` needed no code fix — its cost is legitimate, volume-proportional
work (Faker calls, JSONB demographics generation, chaos injection), fixed by Fix 3 below.

## Fix 3 — resized per-run volume ranges

`configs/seeders/config.py`'s ranges were rescaled ÷100 (documented inline in that file, and
in `CLAUDE.md`'s volume table) — e.g. `sales.salesorder` went from 20k-1M/day to 200-10k/run.
100 is a round number between ~96 runs/day (15min cadence) and ~144 runs/day (10min cadence).
**If the schedule interval changes meaningfully, rescale again** — divisor = target runs/day —
rather than leaving stale ranges sized for a different cadence.

Small, already-bounded tables (`humanresources`, `product` — has its own `absolute_max`) were
left untouched; they were never part of the problem.

## An unrelated gap found and patched along the way: `AuditMixin`'s `created_at` index doesn't apply

`configs/api/base.py`'s `AuditMixin` declares a `created_at` index via `@declared_attr` on
every model, intended to apply automatically to every table. Empirically, it doesn't — checked
directly (`PurchaseOrderAuditModel.__table__.indexes`) and confirmed no `ix_*_created_at`
index exists on any table in the live DB, including ones never touched by the earlier
partitioning migrations (`purchaseorderdetail`, `purchaseorderaudit`) — so this predates
2026-08-17 and isn't a side effect of anything done today. Root cause not investigated
(out of scope for this fix); worked around by creating the needed indexes explicitly
(`configs/postgresql/migrations/006_fanout_seeder_indexes.sql`, plus one added into each of
the 5 partitioning migration scripts from the same day — see `configs/postgresql/README.md`).
Worth a real fix at some point since every table in the system is silently missing this index,
not just the ones this session happened to depend on.

## Files touched

- `dbapi/api/seeder/person/person_customer_email.py`
- `dbapi/api/seeder/purchasing/purchase_purchaseorderdetail.py`
- `dbapi/api/seeder/purchasing/purchase_purchaseorderaudit.py`
- `dbapi/api/seeder/sales/sales_order.py`
- `dbapi/api/seeder/cli_common.py` (new `sample_fk_ids()` helper)
- `dbapi/api/models/person/customer_email.py`, `dbapi/api/models/purchasing/purchaseorderdetail.py`,
  `dbapi/api/models/purchasing/purchaseorderaudit.py` (new indexes, declared for future fresh DBs)
- `configs/seeders/config.py` (resized ranges)
- `CLAUDE.md` (volume table updated to match)
- `configs/postgresql/migrations/006_fanout_seeder_indexes.sql` (new — retrofits the above
  indexes onto an existing DB) and `001`-`005` (added the missing `created_at` index to each)

## What wasn't done / follow-ups

- **`nexomart_seeder_dag`'s `schedule` is still `"0 2 * * *"`** — not changed as part of this
  fix. The ranges above are calibrated for a ~10-15min cadence, but flipping the actual cron
  is a separate, more consequential step (starts continuous background load) left for the
  user to do deliberately.
- **`AuditMixin`'s broken `created_at` index** (see above) — worked around, not fixed at the
  root.
- **`purchase_purchaseorderdetail.py`'s `existing_count` query** (`SELECT COUNT(*) FROM
  purchaseorderdetail`) still runs unconditionally every call even though
  `purchasing.purchaseorderdetail`'s `absolute_max` is currently `None` (making the check a
  no-op). Minor, not part of the slow-task list, left alone.

## Follow-up session — 2026-08-20/21: closing the gaps, plus new problems found running the DAG for real

Adding `max_active_runs=1` + `dagrun_timeout=timedelta(minutes=15)` to the DAG (the two
follow-ups above, closed first) let the user actually trigger `nexomart_seeder_dag` repeatedly
and watch it in real time — which surfaced a chain of problems the 2026-08-17 session's
measurements hadn't caught, because those were taken running seeders individually via
`docker exec`, not through a live, repeated Airflow run against a database that had already
accumulated real history. Every fix below was verified against the live DB, not assumed.

### Problem 1 — the same "unconditional COUNT(*)" bug, missed in 5 more seeders

`purchase_purchaseorderdetail.py`'s `existing_count` COUNT(*) (flagged as a followup above) got
fixed, and while fixing it the same exact pattern — `existing_count = await session.scalar(select(func.count(...)))`
running unconditionally even though `ranges.absolute_max` is `None` for that table — was found
copy-pasted into 5 more seeders: `person_customer.py`, `purchase_purchaseorderheader.py`,
`sales_order.py`, `sales_return_order.py`, `transaction_history.py`. All five were guarding a
no-op check (`if ranges.absolute_max and existing_count >= ranges.absolute_max`) with a query
that touches the whole table regardless.

**Measured impact on `person_customer.py`** (the worst case, since `person.customer` is
LIST-partitioned and had ~2.15M rows at the time): the `COUNT(*)` alone took **11m25s** — logged
timestamp of the `SELECT count(...)` statement to the next log line (`📣 Seeding...`) with zero
work in between. Actual row generation + insert for the same run took ~70s. This is what the
2026-08-17 session's doc (Fix 3 above) wrongly attributed to "legitimate Faker/JSONB cost" —
the seeder was never the bottleneck, this no-op query was.

**Fix**: same pattern as the original `purchase_purchaseorderdetail.py` fix — only run the
`COUNT(*)` when `ranges.absolute_max` is actually set; otherwise skip it and print `n/a` for the
"final total" in the log line. **Measured after fix**: `person_customer.py` end-to-end dropped
from ~12-13min to **5m28s** for the same task, in the same live run.

### Problem 2 — two more full-column FK loads, same class as 2026-08-17's Fix 2

`sample_fk_ids()` (the `TABLESAMPLE SYSTEM` helper from Fix 2 above) was applied to
`sales_order.py`'s `customer_ids` on 2026-08-17, but a sweep of every seeder's
`.scalars().all()` calls found two more unbounded full-column loads of the same growing table
(`sales.salesorder`, now past 1M rows) that were missed:

- `warehouse/transaction_history.py`: `order_ids = (await session.execute(select(SalesOrderModel.salesorder_id))).scalars().all()`
- `sales/sales_eventlog.py`: same query, then `random.sample()`'d in Python — `sample_fk_ids()`
  already returns a bounded random sample, so the extra `random.sample()` on top was also
  removed as redundant.

Every other `.scalars().all()` call across all seeders was audited and left alone — they're all
small/bounded master data (`product` capped at 100k, `humanresources`, warehouse locations,
transaction types, sales channels/offers/sellers, countries) where a full load is genuinely
cheap. `product_embedding.py`'s fan-out (`already_embedded = set(...)`) is architecturally the
same "full diff in Python" pattern the 2026-08-17 session replaced with high-water-marks for
`customer_email`/`purchase_detail`/`purchase_audit` — but since `product` has a hard
`absolute_max=100_000`, the diff never scales past ~100k×2 integers, so it wasn't touched here.

### Problem 3 — corrupted B-tree index on `person.customer_br`

A live run's `dimensions.person_customer` task failed with:
```
asyncpg.exceptions.IndexCorruptedError: right sibling's left-link doesn't match:
block 396 links to 3510 instead of expected 1666 in index "customer_br_row_guid_country_id_key"
```
Almost certainly a consequence of `fsync=off`/`full_page_writes=off` (see
`configs/postgresql/README.md` — a risk the user explicitly accepted for this synthetic-data
environment) combined with an earlier hard kill of the same task (`SIGKILL`, "zombie job",
found in an older run's log from 2026-08-17) — an interrupted write with `full_page_writes=off`
can leave a B-tree page inconsistent. **Fix**: `REINDEX TABLE person.customer_br;` — no schema
change, just rebuilds the index. Retried automatically by Airflow before the manual fix and
happened to succeed once, which does *not* mean the corruption was gone — confirmed fixed via
`pg_index.indisvalid` only after the explicit `REINDEX`.

### Problem 4 — `insert_in_batches()` vs `bulk_copy()` for the two remaining fan-out seeders

`bulk_copy()` (Postgres `COPY` protocol, see cli_common.py) was already used by the
highest-volume seeders since 2026-08-17, but `person_customer_email.py` and
`purchase_purchaseorderaudit.py` still used `insert_in_batches()` (batched `INSERT...VALUES`).
The original reason to prefer INSERT over COPY — `bulk_copy()` not applying `AuditMixin`'s
Python-side defaults like `row_version=1` (caught 2026-08-13, see Fix 1 above) — no longer
applies, since `bulk_copy()` now handles that itself. Both seeders' payloads were confirmed
`bulk_copy()`-compatible (no `ON CONFLICT`, no JSONB columns, plain flat dicts) before
switching, including a targeted check that asyncpg's binary `COPY` accepts plain `str(uuid4())`
values for `uuid` columns (it does — verified directly, not assumed).

**Measured**: `person_customer_email.py` — before, 6 batches of 1000 rows via
`insert_in_batches()` took ~1m40s-2min *each* and didn't finish within the DAG's 15min budget;
after switching to `bulk_copy()`, all 11,967 rows (10,000 customers × ~1.2 emails/customer)
inserted as a single COPY in **2m49s** end-to-end. `purchase_purchaseorderaudit.py`: 22,927 rows
in **5m16s** via `bulk_copy()` (not separately measured before, since it never got far enough to
finish under `insert_in_batches()` during a live run).

### Problem 5 — the real reason `ANALYZE` had never run, and `/dev/shm` too small for `maintenance_work_mem`

Investigating the `insert_in_batches()` slowness above led to inspecting the affected tables'
real size, and found `pg_stat_user_tables.n_live_tup`/`n_dead_tup` were wildly wrong for several
of them — e.g. `person.customer_email` showed 12,023 live rows (actual: 2,580,897), because
`last_analyze`/`last_autoanalyze` were both `NULL`: **`ANALYZE` had never successfully run** on
these tables, ever. A manual `VACUUM (VERBOSE, ANALYZE)` reproduced why:
```
ERROR:  could not resize shared memory segment "/PostgreSQL.xxxxxxxxxx" to N bytes: No space left on device
```
Root cause: the 2026-08-09 GUC tuning session (see `configs/postgresql/README.md`) set
`maintenance_work_mem=1GB`, but `pgprojeto`'s container never had `shm_size` set, so Docker
defaulted it to 64MB — nowhere near enough for the shared-memory dead-tuple tracking structure
a 1GB-`maintenance_work_mem` `VACUUM`/`ANALYZE` (parallel or not) needs. Autovacuum uses the
same `maintenance_work_mem` by default (`autovacuum_work_mem` was unset), so it had almost
certainly been failing silently the same way since 2026-08-09 — `autovacuum_count=0` on every
table checked, `pg_postmaster_start_time` showed a recent restart (stats reset), but the dead
tuples themselves (data-level, not just stats) predated that restart.

**Immediate unblock** (same psql session, no infra change): `SET max_parallel_maintenance_workers = 0;`
before `VACUUM (ANALYZE);` avoids requesting a parallel worker's DSM segment — ran a full-database
`VACUUM ANALYZE` this way to refresh every table's stats immediately.

**Durable fix**: `postgres/docker-compose.yml` — added `shm_size: 2gb` to the `pgprojeto`
service. Bloat itself, once real numbers were visible, turned out to be modest everywhere
checked (0.1-2.1% dead tuples) — the earlier "625MB for 12k rows" read as severe bloat was an
artifact of the wrong row count, not real bloat. The user separately pointed out that
`shared_buffers=1GB` + the new `shm_size=2GB` already commits 3GB of a 4GB `mem_limit`, leaving
thin headroom for `work_mem` (128MB) × concurrent connections + autovacuum workers under the
DAG's real concurrent load — so `mem_limit` was raised `4g` → `6g` in the same file. Applying
either setting requires recreating the container (`docker compose up -d --force-recreate --wait
pgprojeto`), not just a restart — `shm_size`/`mem_limit` are set at container-creation time.

**Operational gotcha found while doing this**: recreating `pgprojeto` breaks `airflow_scheduler`'s
DB connection hard enough to crash the scheduler process outright (`OperationalError: connection
... failed: Connection refused` / `FATAL: the database system is shutting down`), not just retry.
The first time it required a manual `docker start airflow_scheduler`; every recreate after that
(3 more, across Problems 5 and 6) self-relaunched its `DagFileProcessorManager` and recovered
without help. Either way: **any future `pgprojeto` recreate should be followed by checking
`airflow_scheduler`'s container status**, not assumed to survive gracefully.

### Problem 6 — I/O contention under concurrent load, and the timeout that was making it worse

With Problems 1-5 fixed, `dagrun_timeout=15min` (added earlier this session, see the two
follow-ups closed at the top of this section) still killed the `derived` TaskGroup — but this
time the killed tasks (`purchase_detail`, `purchase_audit`, `transaction_history`) hadn't logged
*any* progress, not even their first query's result, for the full ~12.5 minutes before being
killed. Checking `pg_stat_activity` immediately after the kill was the key step: the backend
queries were **still running server-side**, `state='active'`, `wait_event='DataFileRead'`,
13-14 minutes in — Airflow's `SIGTERM` on `dagrun_timeout` only kills the client-side Python
process, not the in-flight Postgres backend, so a timed-out run leaves orphaned `COPY`/`SELECT`
statements still consuming real I/O for minutes after Airflow already marked the task "skipped".
That's a feedback loop: timeout kills a task, kill doesn't stop the query, orphaned query keeps
competing for I/O with whatever the *next* run's tasks try to do.

Root cause: several heavy `bulk_copy()` tasks (`purchase_detail`, `purchase_audit`, into
tables with 3-4M rows and several indexes each) landing in the same `derived`/`facts` window
with no concurrency limit (`max_active_tasks=16`, unset at the DAG level) — all fighting for the
same disk. Measured the disk itself first rather than assume it was just "slow": sequential
write via `dd` was **187MB/s**, a perfectly healthy number — so this isn't broken storage, it's
under-tuned I/O settings (still at Postgres's HDD-era defaults: `effective_io_concurrency=1`,
`random_page_cost=4`) plus real contention from concurrency, running on a Docker
Desktop/WSL2-backed virtual disk shared with every other container in the stack (Trino, MinIO,
Spark, Hive, Unity Catalog, all three Airflow containers).

**Fixes applied**:
1. `dagrun_timeout` removed entirely (was 15min, briefly raised to 45min, then removed) — the
   timeout was actively making contention worse per the feedback loop above, and no fixed budget
   made sense until the real, uncapped duration was known.
2. Postgres I/O tuning for SSD/VM-backed storage instead of HDD-era defaults:
   `effective_io_concurrency=1→200`, `maintenance_io_concurrency=10→200`, `random_page_cost=4→1.1`
   (new `PG_*` vars in `configs/secrets.env`/`secrets.env.example`, new `-c` lines in
   `postgres/docker-compose.yml`'s `command:`). `shared_buffers` also bumped `1GB→1536MB` to keep
   the already-documented "25% of `mem_limit`" ratio after Problem 5's `mem_limit: 4g→6g`.
3. **Not done**: throttling `max_active_tasks` on the DAG. This is the more direct fix for
   contention specifically (fewer heavy I/O tasks fighting at once) — the user chose to relax the
   timeout and observe real behavior first instead. Worth revisiting if steady-state timing is
   still unsatisfactory once the schedule actually moves to 10-15min (see "what's still open").

**Result — first full run to ever complete clean end-to-end**: `manual__2026-08-21T01:08:04`,
all 31 tasks `success`, zero skips, zero failures, **18m36s** total (01:08:05 → 01:26:41). Every
previously-slow task individually confirmed fast: `person_customer` 12m49s (still the slowest —
runs concurrently with `purchase_detail`/`purchase_audit`, so still absorbing some contention),
`purchase_audit` 8m02s, `purchase_detail` 9m14s, `sales_order` 4m21s, `person_customer_email`
4m39s, `sales_return_order` 8s, `transaction_history` 1m17s, `sales_eventlog` 1s. This is the
first real number to calibrate a future timeout against — see "what's still open".

### Files touched (this follow-up session)

- `configs/api/base.py` — `AuditMixin.created_at`'s `@declared_attr` `Index(...)` (never
  actually attached to any table — string column reference only resolves via `__table_args__`,
  which every real model overrides for schema-qualification) replaced with `index=True` directly
  on the `Column`, verified working on schema-qualified/partitioned models.
- `dbapi/api/seeder/purchasing/purchase_purchaseorderdetail.py`,
  `dbapi/api/seeder/person/person_customer.py`,
  `dbapi/api/seeder/purchasing/purchase_purchaseorderheader.py`,
  `dbapi/api/seeder/sales/sales_order.py`,
  `dbapi/api/seeder/sales/sales_return_order.py`,
  `dbapi/api/seeder/warehouse/transaction_history.py` — guarded `COUNT(*)` behind
  `ranges.absolute_max`.
- `dbapi/api/seeder/warehouse/transaction_history.py`,
  `dbapi/api/seeder/sales/sales_eventlog.py` — `order_ids` switched to `sample_fk_ids()`.
- `dbapi/api/seeder/person/person_customer_email.py`,
  `dbapi/api/seeder/purchasing/purchase_purchaseorderaudit.py` — `insert_in_batches()` →
  `bulk_copy()`.
- `airflow/dags/nexomart/seeder/nexomart_seeder_dag.py` — added `max_active_runs=1` (prevents
  overlapping runs, which the fan-out seeders' high-water-mark logic implicitly assumes);
  `dagrun_timeout` added at 15min, raised to 45min, then removed entirely (see Problem 6).
- `postgres/docker-compose.yml` — `shm_size: 2gb`, `mem_limit: 4g` → `6g` (Problem 5);
  `effective_io_concurrency`/`maintenance_io_concurrency`/`random_page_cost` added, `shared_buffers`
  `1GB` → `1536MB` (Problem 6) — all four via new `-c` lines reading `PG_*` env vars.
- `configs/secrets.env`, `configs/secrets.env.example` — `PG_SHARED_BUFFERS=1536MB`,
  new `PG_EFFECTIVE_IO_CONCURRENCY=200`, `PG_MAINTENANCE_IO_CONCURRENCY=200`,
  `PG_RANDOM_PAGE_COST=1.1`.
- Live DB: `REINDEX TABLE person.customer_br;`, full-database `VACUUM ANALYZE`.

### Problem 7 — re-testing the person_customer/purchase_audit/purchase_detail serialization edge

Problem 6's fix had two parts: reducing `max_active_tasks` from the unset default (16) to 8
DAG-wide, and adding an artificial dependency edge
(`person_customer >> purchase_audit >> purchase_detail`) forcing those three specific heavy
`bulk_copy()` tasks to run one at a time — not because any of them actually reads the others'
data (verified: neither purchasing seeder references customer data), purely to stop them
fighting over disk I/O.

Since that fix, two infrastructure changes landed that directly target the same bottleneck:
Postgres's data volume moved to a dedicated SSD-backed disk (was sharing a disk with everything
else in Docker Desktop/WSL2), and `effective_io_concurrency`/`maintenance_io_concurrency`/
`random_page_cost` were tuned for SSD-class random access instead of Postgres's HDD-era
defaults. Neither change was specifically test-driven against this contention at the time.

Re-tested 2026-09-05 by removing the serialization edge and watching a live run
(`scheduled__2026-09-05T11:40:00+00:00`, `nexomart_seeder_dag`) with `max_active_tasks=8` left
unchanged:

```
dimensions.person_customer   11:47:34.75 -> 11:48:29.84   (55s)
derived.purchase_audit       11:47:55.81 -> 11:48:35.46   (40s)  <- started while person_customer still running
derived.purchase_detail      11:47:55.83 -> 11:48:09.93   (14s)  <- same overlap
```

All three genuinely overlapped in wall-clock time (`purchase_audit`/`purchase_detail` both
started at 11:47:55, a full 21s before `person_customer` finished) with no inflation — nothing
close to the old 5m28s-isolated-vs-12m49s-concurrent pattern. Kept the edge removed based on
this. One clean run isn't exhaustive proof (seeded volumes are randomized per run, see
`configs/seeders/config.py`), so this should keep being watched over the next several scheduled
runs before fully trusting it — restore
`person_customer >> purchase_audit >> purchase_detail` in `nexomart_seeder_dag.py`'s `derived`
TaskGroup section if any of these three inflate again.

`max_active_tasks=8` was left as-is — this re-test only concerned the specific 3-task edge, not
the DAG-wide cap (a separate, larger change that would need its own controlled test — see the
now-stale "max_active_tasks is still unset" bullet below, corrected here: it's been 8 since
2026-08-21, and there's no current evidence it needs to move again).

### What's still open / follow-ups

- **`nexomart_seeder_dag`'s `schedule` is still `"0 2 * * *"`** — same follow-up as before, still
  not changed. Now that a full run has a real measured duration (18m36s, see Problem 6), this is
  the next thing blocking the actual move to a 10-15min cadence: 18m36s doesn't fit inside a
  10-15min schedule interval on its own merits, before even accounting for run-to-run variance.
- **No `dagrun_timeout` at all right now** — deliberately removed in Problem 6 so real behavior
  could be observed without a safety net's side effects (see the feedback-loop description
  there). This is intentional for now but not meant to be permanent: once steady-state timing is
  well understood (ideally a few more natural runs, not just one), a timeout sized to that
  reality (with real margin, e.g. 1.5-2x observed) should come back — running indefinitely with
  no ceiling at all is a real risk once this DAG is actually on a tight schedule.
- **RESOLVED 2026-09-05** (was: "`max_active_tasks` is still unset") — `max_active_tasks` has
  been 8 (not the unset 16 default) since 2026-08-21, and Problem 7 above re-tested and removed
  the `person_customer >> purchase_audit >> purchase_detail` serialization edge specifically.
- **`product_embedding.py`'s full-diff fan-out pattern** — noted in Problem 2, left alone since
  it's bounded by `product`'s `absolute_max=100_000`, but stylistically inconsistent with the
  high-water-mark pattern used everywhere else; would need the same treatment if `product`'s
  ceiling is ever raised or removed.
- **`airflow_scheduler`'s fragility on a `pgprojeto` restart** (see Problem 5's operational
  gotcha, confirmed again 3 more times in Problem 6) — not investigated further; Airflow's
  scheduler is generally expected to retry transient DB connection loss rather than crash, so
  this may be a config gap worth a real fix later.
- **RESOLVED** (was: "NVMe/HDD disk split, discussed but not applied") — `pgprojeto`'s volume
  now lives on a dedicated SSD-backed disk (`postgres/docker-compose.yml`:
  `/mnt/wsl/postgres-ssd/pgdata`), separate from the rest of the stack. See Problem 7 above for
  the re-test this enabled. MinIO/Airflow/Spark logs were not split onto separate storage.
