# `nexomart_mutator_dag` — three real bugs found and fixed

`dbapi/api/seeder/*/*_mutate.py` (16 scripts, `mutator_common.py` shared driver) sample
already-seeded rows and UPDATE or DELETE them, to exercise a future Debezium/CDC pipeline with
a realistic mix of c/u/d WAL events instead of insert-only traffic. The DAG that orchestrates
them (`airflow/dags/nexomart/mutator/nexomart_mutator_dag.py`) was reported failing on every
task. Written 2026-09-04.

## Bug 0 — the DAG had no dependencies at all

`start_ingestion`/`end_ingestion` (leftover naming from a copy-paste of the ingestion DAG — the
tasks are really `start`/`end` for the mutator run) and all 16 mutate tasks were created but
never wired together with `>>`. In practice this meant `end_ingestion` "succeeded" instantly
(`start_date == end_date`) regardless of whether any mutate task had even started, and nothing
gated the mutate tasks on `start` finishing first. Fixed:

```python
start_ingestion >> ingest_tasks >> end_ingestion  # ingest_tasks = the list of all 16
```

`end_ingestion`'s default `trigger_rule` (`all_success`) now means it only succeeds once every
mutate task has — a genuine gate, not a decoration.

## Bug 1 — missing FK model imports (13 of 16 scripts)

SQLAlchemy only registers a model's `Table` on `Base.metadata` once that model class has
actually been imported somewhere in the running process. Each `_mutate.py` script only imported
the one model it directly mutates — but `run_mutation()`'s bulk UPDATE needs to resolve every
FK on that table to sort tables topologically, and if the FK's *target* model was never
imported, that raises at runtime, not at import time:

```
sqlalchemy.exc.NoReferencedTableError: Foreign key associated with column
'employee.state_province_id' could not find table 'person.stateprovince' ...
```

The corresponding insert-side seeder (e.g. `humanresources_employee.py`) already imports every
FK target it needs, for the same reason — the mutate scripts just hadn't been kept in sync.
Fixed by adding the missing imports (marked `# noqa: F401`, since they're required for
side-effect registration, not used directly) to all 13 affected scripts: `humanresources_
employee`, `marketing_campaign`, `marketing_daily_performance`, `person_customer`, `product`,
`purchase_purchaseorderaudit`, `purchase_purchaseorderdetail`, `purchase_purchaseorderheader`,
`sales_order`, `sales_return_order`, `sales_seller`, `product_inventory`, `transaction_history`.

A first automated pass to find these (grepping each model's `ForeignKey(...)` lines) missed two
real ones — `sales_return_order`'s FKs to both `person.seller` and `product.product` — because
the regex only reliably caught the first `ForeignKey` in a file. The reliable check is manual:
`grep -n "ForeignKey" <model file>` and cross-reference by eye, not a one-shot script.

## Bug 2 — `bulk_hard_delete()` silently compiled to `DELETE FROM <table>` with no `WHERE`

SQLAlchemy 2.0 supports ORM-enabled bulk UPDATE via `session.execute(update(Model), rows)` (a
list of dicts, one executemany call per batch — `bulk_update()` already used this, correctly).
The equivalent for DELETE does not exist:

```
sqlalchemy.exc.InvalidRequestError: Bulk ORM DELETE not supported right now. Statement may be
invoked at the Core level using session.connection().execute(stmt, parameters)
```

The obvious-looking fix — swap `session.execute` for `(await session.connection()).execute()`,
keep everything else the same — is a real trap. Core-level `delete(model)` has no ORM magic to
turn a list of PK dicts into a `WHERE` clause the way the ORM bulk UPDATE path does. Confirmed
live via SQL echo:

```
[SQL: DELETE FROM purchasing.vendor] [parameters: [(), (), (), (), ()]]
```

No `WHERE` clause at all, and the parameter dicts bind to nothing — an **unconditional delete of
the entire table**, on every call. It only failed loudly here because an FK constraint happened
to block it (`purchaseorderheader` still referenced the sampled vendor); a table with no inbound
FKs would have silently lost every row it had. Real fix — build the `WHERE` explicitly, matching
each PK column to a same-named bind parameter so the executemany-style dicts bind correctly
(generalizes to the composite PKs on the 5 partitioned tables the same way `sample_active_pks()`
already does):

```python
pk_cols = [c.name for c in model.__table__.primary_key.columns]
stmt = delete(model).where(
    and_(*(model.__table__.c[col] == bindparam(col) for col in pk_cols))
)
conn = await session.connection()
for i in range(0, len(pk_rows), batch_size):
    await conn.execute(stmt, pk_rows[i:i + batch_size])
```

Verified by checking exact row-count deltas before/after a controlled `--delete-ratio 1.0
--count 3` run, for both a single-column PK (`warehouse.productinventory`: 173379 → 173376) and
a composite PK (`warehouse.transactionhistory`: 8240740 → 8240737) — both dropped by exactly 3,
and the SQL echo showed a real `WHERE ... = $1` with real bound values, not an empty tuple.

Code: `dbapi/api/utils/mutator_common.py` — `bulk_hard_delete()`.

## Bug 3 — hard-deleting a row that's still an FK target

Once bug 2 was fixed and actually generating a scoped `DELETE ... WHERE pk = $1`, a *different*,
completely legitimate error started showing up for specific tables:

```
IntegrityError: update or delete on table "vendor" violates foreign key constraint
"purchaseorderheader_vendor_id_fkey1" on table "purchaseorderheader"
```

`sample_active_pks()` picks rows at random with no awareness of whether other tables still
reference them. For a table nobody else points at (`purchaseorderheader`, `transactionhistory`,
`purchaseorderdetail`, ...) that's fine — for one that's a genuine FK *target* of another table,
random sampling will eventually pick a still-referenced row and the whole batch's `DELETE`
fails. Five tables hit this: `product` (← `specialofferproduct`), `humanresources.employee` (←
`purchaseorderaudit`), `purchasing.vendor` (← `purchaseorderheader`), `marketing.marketingcampaign`
(← `marketingadgroup`), `person.seller` (← `returnorder.store_id`).

Considered filtering the sample to exclude referenced rows (more correct, more complex — would
need a per-table subquery against every table that FKs into it) versus just not hard-deleting
these five at all. Went with the latter: switched all five to
`HARD_DELETE_ALLOWED = False`, the same soft-delete-only (`is_active = false`) treatment the
`sales.*`/audit-type tables already use for a different reason (business rule: those never get a
real DELETE). It's also the more realistic chaos scenario either way — an employee who left, a
vendor relationship that ended, a campaign that's over — soft-deleting instead of hard-deleting
matches what a real system would do for a still-referenced master-data row.

## Bug 4 — hard-delete on a fan-out *parent* silently orphans its children, then collides on ID reuse

Bug 3 covered tables that are FK targets *today* (the constraint still exists, so Postgres
catches it). Two tables are logical fan-out parents (CLAUDE.md's fan-out seeder list:
`person_customer_email.py`, `purchase_purchaseorderdetail.py`, `purchase_purchaseorderaudit.py`)
whose FK **no longer exists at the DB level at all** — the partitioning migration
(`configs/postgresql/README.md`) dropped it from `purchasing.purchaseorderheader` and
`person.customer` when both were converted to composite-PK partitioned tables ("enforced by the
seeders only" was already a documented tradeoff there). That means a hard `DELETE` on either
table succeeds with **zero error**, leaving `purchaseorderdetail`/`purchaseorderaudit`/
`customer_email` rows pointing at a header/customer ID that no longer exists — and later, when a
seeder randomly generates a *new* row that happens to reuse that same business ID, the fan-out
seeder tries to insert detail/audit rows for the "new" header and hits a real `UniqueViolation`
on the old orphaned rows' PK:

```
asyncpg.exceptions.UniqueViolationError: duplicate key value violates unique constraint
"purchaseorderdetail_pkey"
DETAIL:  Key (purchaseorderdetail_id)=(PO-368334211-01) already exists.
```

Found live 2026-09-04 via `nexomart_seeder_dag`'s `derived.purchase_detail` task failing on
exactly this. Confirmed the mechanism precisely: header `PO-368334211` had
`created_at=2026-09-04 18:46:05` while its 3 "existing" detail rows had
`created_at=2026-08-25 17:07:12` — impossible unless the original header was hard-deleted and a
new one later landed on the same ID. Quantified the damage with anti-join queries: **1,508**
orphaned `purchaseorderdetail` rows, **9,667** orphaned `purchaseorderaudit` rows, **968**
orphaned `person.customer_email` rows (parent genuinely gone, not reused) — plus **2 specific
ID-reuse collisions** (`PO-368334211`, `PO-114221573`) where the parent ID *was* reused, so a
plain "parent missing" anti-join doesn't even catch them (a header now exists at that ID — it's
just not the same header the orphaned children belong to). Cross-checked every other partitioned
table (`sales.salesorder`, `warehouse.transactionhistory`, `marketing.dailyperformance`) has no
fan-out children, so this exposure is unique to these two tables.

Fix, same shape as Bug 3: `HARD_DELETE_ALLOWED = False` on `person/person_customer_mutate.py` and
`purchasing/purchase_purchaseorderheader_mutate.py`. Soft-delete matches the treatment
`purchaseorderaudit` already gets for a different reason, and is the more realistic "employee
left" / "right to be forgotten" scenario anyway — deactivate, don't physically erase, while
dependents exist. Verified live with `--delete-ratio 1.0 --count 5` on both scripts: clean
`UPDATE ... SET is_active=false` with a real bound `WHERE`, no more `DELETE`.

**Cleanup of the already-orphaned data** needed two passes, not one:
1. Plain anti-join (`NOT EXISTS` on the parent) caught the 1,508 / 9,667 / 968 rows whose parent
   is genuinely gone — safe to delete outright.
2. The 2 ID-reuse collisions needed a *different* test (`child.created_at < parent.created_at`,
   since detail/audit rows are always created in the same seeder run as their header, so a child
   older than its own header's `created_at` is impossible otherwise) — and critically, this test
   had to be scoped to the specific header IDs it flagged (2 of them), not run broadly. Running
   `child.created_at < parent.created_at` unscoped against the full `purchaseorderaudit` table
   matched **42 million** rows — audit rows are legitimately backdated to track the order's
   business lifecycle (`order_date`), not insert time, so most of the table trivially satisfies
   that condition with no bug involved. Scoping the delete to just the 2 flagged header IDs
   (`PO-368334211`, `PO-114221573`) removed exactly 5 stale detail rows + 146 stale audit rows —
   the real fix — instead of nearly wiping the table.

## Result

All 16 mutate tasks + `start`/`end` — clean `success`, full DAG run in 11 seconds (2026-09-04,
run `manual__2026-09-04T19:23:54`). Verify any specific script still concerns you with a forced
delete: `--delete-ratio 1.0 --count 5` reliably exercises the delete path (`--random`/default
delete ratios can be too low to hit it in a quick manual check).

`nexomart_seeder_dag` had backed up 42 `queued` `DagRun`s while Bug 4 was live (every run's
`derived.purchase_detail`/`purchase_audit` hit the same 2 collisions, and `max_active_runs=1`
means the next tick can't start until the current one finishes). Considered raising
`max_active_runs` to the platform default (16) or `max_active_tasks` beyond its current 8 to
drain the backlog faster; rejected both — `max_active_runs=1` is load-bearing (prevents seeder
runs from racing each other and guarantees `nexomart_ingestion_dag` reads a stable snapshot, see
`nexomart_seeder_dag.py`'s docstring), and `max_active_tasks=8` is already at the real ceiling
(`airflow-worker`'s Celery `worker_concurrency=8`) — raising it without also raising
`worker_concurrency` would do nothing, and raising `worker_concurrency` would reopen the I/O
contention `docs/SEEDER_MICROBATCHING.md`'s Problem 6 already fixed once. Left both settings
alone; the scheduler's own backlog handling resolved the 42 stale runs as `failed` (no
`start_date` — they never actually executed) once the underlying data was cleaned up, and fresh
ticks resumed normally.
