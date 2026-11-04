import io
import json

import pyarrow.parquet as pq
import pytest
from api.utils.quarantine import Quarantine, QuarantineLimitExceeded
from pydantic import BaseModel, Field
from sqlalchemy import BigInteger, Column, Integer, MetaData, Numeric, String, Table


class Item(BaseModel):
    item_id: int = Field(gt=0)
    code: str = Field(pattern=r"^\d{3}$")


class FakeMinio:
    def __init__(self, ok=True):
        self.ok = ok
        self.objects = {}

    def __call__(self, bucket, object_name, data):
        if self.ok:
            self.objects[(bucket, object_name)] = data
        return self.ok


def _validate(q, rows):
    out = []
    for payload in rows:
        with q.row(payload):
            out.append(Item(**payload).model_dump())
    return out


def test_valid_rows_pass_and_nothing_is_written():
    minio = FakeMinio()
    q = Quarantine("sales.item", writer=minio)
    out = _validate(q, [{"item_id": 1, "code": "123"}, {"item_id": 2, "code": "456"}])
    q.finish()
    assert len(out) == 2 and q.accepted == 2 and not minio.objects


def test_rejected_row_goes_to_parquet_with_payload_column():
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=0.5, writer=minio)
    bad = {"item_id": 2, "code": "12"}
    out = _validate(q, [{"item_id": 1, "code": "123"}, bad])
    q.finish()

    assert out == [{"item_id": 1, "code": "123"}]  # the bad row is not returned
    (bucket, name), data = next(iter(minio.objects.items()))
    assert bucket == "quarantine" and name.startswith("nexomart/sales/item/")
    row = pq.read_table(io.BytesIO(data)).to_pylist()[0]
    assert json.loads(row["payload"]) == bad
    assert row["table_name"] == "sales.item"
    assert "code" in row["errors"]


def test_non_validation_errors_still_propagate():
    q = Quarantine("sales.item", writer=FakeMinio())
    with pytest.raises(KeyError):
        with q.row({}):
            raise KeyError("boom")


def test_small_batch_over_the_limit_fails_in_finish():
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=0.01, writer=minio)
    _validate(q, [{"item_id": 1, "code": "123"}, {"item_id": 2, "code": "x"}])
    with pytest.raises(QuarantineLimitExceeded):
        q.finish()
    assert len(minio.objects) == 1  # rejected rows are saved before failing


def test_high_reject_rate_fails_mid_run_once_sample_is_big_enough():
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=0.01, writer=minio)
    with pytest.raises(QuarantineLimitExceeded):
        _validate(q, [{"item_id": 1, "code": "bad"}] * 200)
    assert q.rejected_total < 200  # stopped early, did not process the whole batch
    assert len(minio.objects) == 1


def test_failed_minio_write_fails_the_task_instead_of_dropping_rows(monkeypatch):
    monkeypatch.setattr("api.utils.quarantine.WRITE_BACKOFF_SECONDS", 0)
    q = Quarantine("sales.item", max_reject_rate=1.0, writer=FakeMinio(ok=False))
    _validate(q, [{"item_id": 1, "code": "bad"}])
    with pytest.raises(RuntimeError, match="Could not write quarantine"):
        q.finish()


# ---- physical check: rows the table would refuse, caught after chaos and before the COPY ----

ITEM = Table(
    "item",
    MetaData(),
    Column("item_id", Integer, primary_key=True, autoincrement=True),
    Column("code", String(5), nullable=False),
    Column("note", String(10), nullable=True),
    Column("amount", Numeric(6, 2), nullable=False),
    Column("qty", Integer, nullable=False, default=1),
    Column("big", BigInteger, nullable=True),
    schema="sales",
)
GOOD = {"code": "ABC", "note": "ok", "amount": 12.5, "qty": 2}


def _read(minio):
    (_, _), data = next(iter(minio.objects.items()))
    return pq.read_table(io.BytesIO(data)).to_pylist()


def test_physical_valid_rows_pass_untouched():
    minio = FakeMinio()
    q = Quarantine("sales.item", writer=minio)
    rows = [dict(GOOD), dict(GOOD, code="XYZ")]
    assert q.physical(ITEM, rows) == rows
    assert not minio.objects


@pytest.mark.parametrize(
    "bad, fragment",
    [
        (dict(GOOD, code=None), "NULL in a NOT NULL"),
        (dict(GOOD, code="TOOLONG"), "varchar(5)"),
        (dict(GOOD, amount=1_000_000), "overflows numeric(6,2)"),
        (dict(GOOD, qty=2**31), "out of range"),
        ({k: v for k, v in GOOD.items() if k != "code"}, "NOT NULL with no default"),
    ],
)
def test_physical_rejects_rows_the_table_would_refuse(bad, fragment):
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=1.0, writer=minio)
    kept = q.physical(ITEM, [dict(GOOD), bad])
    assert kept == [dict(GOOD)]
    row = _read(minio)[0]
    assert row["stage"] == "physical"
    assert fragment in row["errors"]
    assert json.loads(row["payload"])["amount"] == bad["amount"]


def test_physical_missing_column_with_a_default_is_fine():
    q = Quarantine("sales.item", writer=FakeMinio())
    row = {"code": "ABC", "amount": 1}  # qty is missing but has a default, item_id is autoincrement
    assert q.physical(ITEM, [row]) == [row]


def test_physical_null_in_nullable_and_bigint_range():
    q = Quarantine("sales.item", writer=FakeMinio())
    row = dict(GOOD, note=None, big=2**40)
    assert q.physical(ITEM, [row]) == [row]


def test_physical_over_the_limit_saves_then_fails_before_returning_rows():
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=0.01, writer=minio)
    rows = [dict(GOOD)] * 98 + [dict(GOOD, code=None)] * 2
    with pytest.raises(QuarantineLimitExceeded, match="2 of 100"):
        q.physical(ITEM, rows)
    assert len(_read(minio)) == 2


def test_both_stages_land_in_the_same_parquet_with_their_stage():
    minio = FakeMinio()
    q = Quarantine("sales.item", max_reject_rate=1.0, writer=minio)
    bad_pydantic = {"item_id": 2, "code": "12"}
    with q.row(bad_pydantic):
        Item(**bad_pydantic)
    with q.row(GOOD):
        pass
    q.physical(ITEM, [dict(GOOD, code=None)])
    assert len(minio.objects) == 1
    assert sorted(r["stage"] for r in _read(minio)) == ["physical", "pydantic"]
