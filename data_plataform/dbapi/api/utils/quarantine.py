"""Quarantine for rows the Pydantic schema or the table would reject, kept out of Postgres.

A rejected row never reaches the database. Its original payload is serialized as JSON into a
`payload` column (outbox style) and written as Parquet to the `quarantine` bucket in MinIO, so it
can be inspected and replayed once the generator or schema bug is fixed.

    q = get_quarantine("person.customer")
    with q.row(payload):                       # wrap the validation + append
        batch.append(Customer(**payload).model_dump())
    ...
    q.finish()                                 # writes the Parquet, enforces the reject-rate limit

Only pydantic ValidationError is quarantined; any other exception still propagates. If the reject
rate exceeds MAX_REJECT_RATE the task fails (a high rate means a generator/schema bug, not stray
bad rows): mid-run once MIN_SAMPLE rows were attempted, and again in finish() for small batches.

Two stages feed the same bucket, told apart by the `stage` column:

  pydantic  a row the schema rejects (type, size, required), caught while it is built
  physical  a row the table would refuse, caught after chaos and right before the COPY
            (NULL in a NOT NULL column, text longer than the varchar, number overflow).
            Without it, one such row aborts the whole COPY. Chaos runs after validation and can
            produce them, so this is the last check before the API -> Postgres boundary.

    rows = q.physical(CustomerModel, rows)     # called for you by bulk_copy / insert_in_batches
"""

import atexit
import io
import json
import os
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Callable, Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError
from sqlalchemy import BigInteger, Integer, Numeric, SmallInteger, String

QUARANTINE_BUCKET = "quarantine"
QUARANTINE_PREFIX = "nexomart"
MAX_REJECT_RATE = float(os.getenv("QUARANTINE_MAX_REJECT_RATE", "0.01"))
MIN_SAMPLE = 50
WRITE_ATTEMPTS = 3
WRITE_BACKOFF_SECONDS = 2


class QuarantineLimitExceeded(RuntimeError):
    pass


def _minio_writer(bucket: str, object_name: str, data: bytes) -> bool:
    from api.config import write_on_minio  # lazy: keeps the module importable without settings

    return write_on_minio(bucket, object_name, data)


def _has_default(col) -> bool:
    return col.default is not None or col.server_default is not None or (
        col.primary_key and col.autoincrement in (True, "auto")
    )


def _physical_problems(table, row: dict) -> list[dict]:
    """What Postgres would refuse in `row` for `table`. Conservative: only checks that always fail
    at the database (NOT NULL, varchar length, numeric overflow, integer range), never guesses at
    type coercions the drivers may accept."""
    problems = []
    for col in table.columns:
        if col.name not in row:
            if not col.nullable and not _has_default(col):
                problems.append({"loc": [col.name], "msg": "missing, column is NOT NULL with no default", "type": "physical"})
            continue
        value = row[col.name]
        if value is None:
            if not col.nullable:
                problems.append({"loc": [col.name], "msg": "NULL in a NOT NULL column", "type": "physical"})
            continue
        t = col.type
        if isinstance(t, String) and t.length and isinstance(value, str):
            if len(value) > t.length:
                problems.append({"loc": [col.name], "msg": f"{len(value)} characters, column is varchar({t.length})", "type": "physical"})
        elif isinstance(t, Numeric) and t.precision is not None and isinstance(value, (int, float, Decimal)) and not isinstance(value, bool):
            try:
                d = Decimal(str(value))
            except InvalidOperation:
                continue
            if d.is_finite() and abs(d) >= Decimal(10) ** (t.precision - (t.scale or 0)):
                problems.append({"loc": [col.name], "msg": f"{value} overflows numeric({t.precision},{t.scale})", "type": "physical"})
        elif isinstance(t, Integer) and isinstance(value, int) and not isinstance(value, bool):
            bits = 63 if isinstance(t, BigInteger) else 15 if isinstance(t, SmallInteger) else 31
            if not -(2 ** bits) <= value < 2 ** bits:
                problems.append({"loc": [col.name], "msg": f"{value} is out of range for {type(t).__name__}", "type": "physical"})
    return problems


class Quarantine:
    def __init__(
        self,
        table: str,
        max_reject_rate: Optional[float] = None,
        writer: Optional[Callable[[str, str, bytes], bool]] = None,
    ):
        self.table = table
        self.max_reject_rate = MAX_REJECT_RATE if max_reject_rate is None else max_reject_rate
        self._writer = writer or _minio_writer
        self.accepted = 0          # rows that passed Pydantic through row()
        self.rejected_pyd = 0
        self.rejected_phys = 0
        self.phys_seen = 0         # rows that reached the physical check
        self._pending: list[dict] = []
        self.last_object: Optional[str] = None
        atexit.register(self._flush_at_exit)

    @property
    def rejected_total(self) -> int:
        return self.rejected_pyd + self.rejected_phys

    def _attempted(self) -> int:
        # Seeders wired through row() see every row there; the physical check only sees the kept
        # ones, so max() also covers seeders that reach the physical check without row().
        return self.rejected_pyd + max(self.accepted, self.phys_seen)

    @contextmanager
    def row(self, payload: dict):
        try:
            yield
        except ValidationError as exc:
            self._reject(payload, exc)
            attempted = self._attempted()
            if attempted >= MIN_SAMPLE and self.rejected_total / attempted > self.max_reject_rate:
                self._fail()
        else:
            self.accepted += 1

    def physical(self, model, rows: list) -> list:
        """Drops the rows the table would refuse into quarantine (stage=physical) and returns the
        rest. Then runs finish(), so a bad rate fails the task before anything is inserted."""
        table = getattr(model, "__table__", model)
        kept = []
        self.phys_seen += len(rows)
        for row in rows:
            problems = _physical_problems(table, row)
            if problems:
                self.rejected_phys += 1
                self._pending.append(
                    {
                        "stage": "physical",
                        "rejected_at": datetime.now(timezone.utc),
                        "payload": json.dumps(row, default=str, ensure_ascii=False),
                        "errors": json.dumps(problems, ensure_ascii=False),
                    }
                )
            else:
                kept.append(row)
        self.finish()
        return kept

    def finish(self) -> None:
        self.flush()
        attempted = self._attempted()
        if self.rejected_total:
            print(
                f"🚧 {self.table}: {self.rejected_total:,} of {attempted:,} rows quarantined "
                f"-> s3://{QUARANTINE_BUCKET}/{self.last_object}"
            )
        if attempted and self.rejected_total / attempted > self.max_reject_rate:
            self._fail()

    def flush(self) -> None:
        if not self._pending:
            return
        rows, self._pending = self._pending, []
        schema_name, _, table_name = self.table.partition(".")
        now = datetime.now(timezone.utc)
        object_name = (
            f"{QUARANTINE_PREFIX}/{schema_name}/{table_name}/{now:%Y-%m-%d}/"
            f"rejected-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:8]}.parquet"
        )
        table = pa.table(
            {
                "table_name": [self.table] * len(rows),
                "stage": [r["stage"] for r in rows],
                "rejected_at": pa.array([r["rejected_at"] for r in rows], type=pa.timestamp("us", tz="UTC")),
                "payload": [r["payload"] for r in rows],
                "errors": [r["errors"] for r in rows],
            }
        )
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        self._upload(object_name, buf.getvalue())
        self.last_object = object_name

    def _reject(self, payload: dict, exc: ValidationError) -> None:
        self.rejected_pyd += 1
        self._pending.append(
            {
                "stage": "pydantic",
                "rejected_at": datetime.now(timezone.utc),
                "payload": json.dumps(payload, default=str, ensure_ascii=False),
                "errors": json.dumps(
                    exc.errors(include_url=False, include_input=False, include_context=False),
                    default=str,
                    ensure_ascii=False,
                ),
            }
        )

    def _upload(self, object_name: str, data: bytes) -> None:
        for attempt in range(1, WRITE_ATTEMPTS + 1):
            if self._writer(QUARANTINE_BUCKET, object_name, data):
                return
            if attempt == WRITE_ATTEMPTS:
                raise RuntimeError(
                    f"Could not write quarantine file s3://{QUARANTINE_BUCKET}/{object_name} "
                    f"after {attempt} attempts; failing instead of dropping rejected rows"
                )
            time.sleep(WRITE_BACKOFF_SECONDS * (2 ** (attempt - 1)))

    def _fail(self) -> None:
        self.flush()
        attempted = self._attempted()
        raise QuarantineLimitExceeded(
            f"{self.table}: {self.rejected_total:,} of {attempted:,} rows rejected "
            f"({self.rejected_total / attempted:.1%} > limit {self.max_reject_rate:.1%}); "
            f"rejected rows are in s3://{QUARANTINE_BUCKET}/{self.last_object}. "
            f"That rate points to a generator or schema bug."
        )

    def _flush_at_exit(self) -> None:
        # Safety net: rows rejected before a crash elsewhere still make it to the bucket.
        try:
            self.flush()
        except Exception as exc:
            print(f"⚠️ Could not flush quarantine for {self.table} at exit: {exc}")


_registry: dict[str, Quarantine] = {}


def get_quarantine(table: str) -> Quarantine:
    """One Quarantine per table per process (each seeder run is its own process)."""
    if table not in _registry:
        _registry[table] = Quarantine(table)
    return _registry[table]


def physical_gate(model, rows: list) -> list:
    """Physical check for seeders that insert with `insert(Model).values(rows)` directly.
    bulk_copy and insert_in_batches already call this."""
    table = getattr(model, "__table__", model)
    name = f"{table.schema}.{table.name}" if table.schema else table.name
    return get_quarantine(name).physical(model, rows)
