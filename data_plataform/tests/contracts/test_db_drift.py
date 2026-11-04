"""Drift check: SQLAlchemy models vs the real Postgres (information_schema and pg_constraint:
columns, types, nullability, primary keys, foreign keys), and the api contracts vs the real Postgres.

test_contract_drift.py compares files with files. This one closes the gap where the model,
the Pydantic schema and the contract were all changed but the ALTER TABLE was forgotten (or the
other way around). It needs a reachable database: without one the tests skip, unless
REQUIRE_DB_DRIFT=1 is set (the airflow-worker gate sets it), in which case they fail.
"""

import importlib
import os
from pathlib import Path

import psycopg2
import pytest
from contract_helpers import REGISTRY, _import, _load, _norm, _props, _sql_physical_type

MODELS_DIR = Path(__file__).resolve().parents[2] / "dbapi" / "api" / "models"


def _model_tables() -> dict:
    from api.config.base import Base

    for f in sorted(MODELS_DIR.rglob("*.py")):
        if f.name == "__init__.py":
            continue
        module = "api.models." + ".".join(f.relative_to(MODELS_DIR).with_suffix("").parts)
        importlib.import_module(module)
    return {name: t for name, t in Base.metadata.tables.items() if t.schema}


MODEL_TABLES = _model_tables()


def _connect():
    from api.config import settings

    return psycopg2.connect(
        host=settings.POSTGREIP,
        port=settings.PGPORT,
        dbname=settings.PGDATABASENAME,
        user=settings.PGUSER,
        password=settings.PGPWD,
        connect_timeout=5,
    )


@pytest.fixture(scope="module")
def db():
    try:
        conn = _connect()
    except Exception as exc:
        if os.getenv("REQUIRE_DB_DRIFT") == "1":
            pytest.fail(f"REQUIRE_DB_DRIFT=1 but Postgres is not reachable: {exc}")
        pytest.skip(f"Postgres not reachable, skipping live drift checks: {exc}")
    yield conn
    conn.close()


def _db_type(data_type, char_len, precision, scale, udt) -> str:
    if data_type == "character varying":
        return f"varchar({char_len})" if char_len else "varchar"
    if data_type == "numeric":
        return f"numeric({precision},{scale})" if precision else "numeric"
    return {
        "timestamp with time zone": "timestamptz",
        "timestamp without time zone": "timestamp",
    }.get(data_type, udt if data_type in ("USER-DEFINED", "ARRAY") else data_type)


def _db_columns(conn, schema: str, table: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, character_maximum_length, numeric_precision,
                   numeric_scale, is_nullable, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return {
            name: {"type": _db_type(dt, cl, p, s, udt), "nullable": nullable == "YES"}
            for name, dt, cl, p, s, nullable, udt in cur.fetchall()
        }


def _db_primary_key(conn, schema: str, table: str) -> set:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname
            FROM pg_constraint c
            JOIN pg_class cl ON cl.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY (c.conkey)
            WHERE c.contype = 'p' AND n.nspname = %s AND cl.relname = %s
            """,
            (schema, table),
        )
        return {r[0] for r in cur.fetchall()}


def _db_foreign_keys(conn, schema: str, table: str) -> set:
    """{(column, 'ref_schema.ref_table.ref_column')} declared on this table in Postgres."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.attname, nf.nspname, cf.relname, af.attname
            FROM pg_constraint c
            JOIN pg_class cl ON cl.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = cl.relnamespace
            JOIN pg_class cf ON cf.oid = c.confrelid
            JOIN pg_namespace nf ON nf.oid = cf.relnamespace
            CROSS JOIN LATERAL unnest(c.conkey, c.confkey) AS k(attnum, fattnum)
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = k.attnum
            JOIN pg_attribute af ON af.attrelid = c.confrelid AND af.attnum = k.fattnum
            WHERE c.contype = 'f' AND n.nspname = %s AND cl.relname = %s
            """,
            (schema, table),
        )
        return {(col, f"{rs}.{rt}.{rc}") for col, rs, rt, rc in cur.fetchall()}


@pytest.fixture(params=sorted(MODEL_TABLES), ids=sorted(MODEL_TABLES))
def model_vs_db(request, db):
    table = MODEL_TABLES[request.param]
    cols = _db_columns(db, table.schema, table.name)
    if not cols:
        pytest.fail(f"{request.param} exists in the models but not in the database")
    return table, cols


def test_columns_match_database(model_vs_db):
    table, db_cols = model_vs_db
    model_cols = {c.name for c in table.columns}
    model_only = sorted(model_cols - set(db_cols))
    db_only = sorted(set(db_cols) - model_cols)
    assert not model_only and not db_only, (
        f"in the model but not in the database (missing ALTER TABLE?): {model_only}; "
        f"in the database but not in the model: {db_only}"
    )


def test_types_match_database(model_vs_db):
    table, db_cols = model_vs_db
    wrong = {
        c.name: {"model": _sql_physical_type(c), "database": db_cols[c.name]["type"]}
        for c in table.columns
        if c.name in db_cols and _sql_physical_type(c) != db_cols[c.name]["type"]
    }
    assert not wrong, f"column types differ between the model and the database: {wrong}"


def test_nullability_matches_database(model_vs_db):
    table, db_cols = model_vs_db
    wrong = {
        c.name: {"model_nullable": c.nullable, "database_nullable": db_cols[c.name]["nullable"]}
        for c in table.columns
        if c.name in db_cols and c.nullable != db_cols[c.name]["nullable"]
    }
    assert not wrong, f"nullability differs between the model and the database: {wrong}"


def test_primary_key_matches_database(model_vs_db, db):
    table, _ = model_vs_db
    model_pk = {c.name for c in table.primary_key.columns}
    db_pk = _db_primary_key(db, table.schema, table.name)
    assert model_pk == db_pk, (
        f"primary key differs: model {sorted(model_pk)}, database {sorted(db_pk)}"
    )


def test_foreign_keys_match_database(model_vs_db, db):
    table, _ = model_vs_db
    model_fks = {(fk.parent.name, fk.target_fullname) for fk in table.foreign_keys}
    db_fks = _db_foreign_keys(db, table.schema, table.name)
    model_only = sorted(model_fks - db_fks)
    db_only = sorted(db_fks - model_fks)
    assert not model_only and not db_only, (
        f"foreign keys in the model but not in the database: {model_only}; "
        f"in the database but not in the model: {db_only}"
    )


@pytest.mark.parametrize("contract_file,model_path", REGISTRY, ids=[r[0] for r in REGISTRY])
def test_contract_matches_database(contract_file, model_path, db):
    contract, _ = _load(contract_file)
    props = _props(contract)
    table = _import(model_path).__table__
    db_cols = _db_columns(db, table.schema, table.name)
    wrong = {
        name: {"contract": prop["physicalType"], "database": db_cols[name]["type"]}
        for name, prop in props.items()
        if name in db_cols and _norm(prop["physicalType"]) != db_cols[name]["type"]
    }
    missing = sorted(set(props) - set(db_cols))
    assert not wrong and not missing, (
        f"contract vs database, type differences: {wrong}; contract fields missing in the database: {missing}"
    )
