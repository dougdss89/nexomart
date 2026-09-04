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

Code: `dbapi/api/seeder/mutator_common.py` — `bulk_hard_delete()`.

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

## Result

All 16 mutate tasks + `start`/`end` — clean `success`, full DAG run in 11 seconds (2026-09-04,
run `manual__2026-09-04T19:23:54`). Verify any specific script still concerns you with a forced
delete: `--delete-ratio 1.0 --count 5` reliably exercises the delete path (`--random`/default
delete ratios can be too low to hit it in a quick manual check).
