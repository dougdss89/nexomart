"""Shared helpers for the contract drift tests (YAML vs Pydantic vs SQLAlchemy vs live database)."""

import enum
import importlib
import types
import typing
import uuid
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import yaml
from pydantic import BaseModel
from sqlalchemy import (
    BigInteger,
    Boolean,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

CONTRACT_ROOT = Path(__file__).resolve().parents[2] / "nexomart_contract" / "api_contract"

# contract path (relative to api_contract/) -> "module:ModelClass"
REGISTRY = [
    ("person/customer.yml", "api.models.person.customer:CustomerModel"),
]

# Columns every table gets from AuditMixin but that are internal, not part of the payload.
DB_ONLY_COLUMNS = {"row_version"}

LOGICAL_TO_PYTHON: dict[str, tuple[type, ...]] = {
    "string": (str, uuid.UUID, enum.Enum),
    "integer": (int,),
    "number": (float, Decimal),
    "boolean": (bool,),
    "date": (date, datetime),
    "object": (BaseModel, dict),
}


def _import(path: str):
    module, name = path.split(":")
    return getattr(importlib.import_module(module), name)


def _load(contract_file: str):
    contract = yaml.safe_load((CONTRACT_ROOT / contract_file).read_text(encoding="utf-8"))
    server = contract["servers"][0]
    custom = {p["property"]: p["value"] for p in server["customProperties"]}
    file_part, class_name = custom["requestSchema"].split("::")
    module = file_part.removeprefix("dbapi/").removesuffix(".py").replace("/", ".")
    return contract, _import(f"{module}:{class_name}")


def _props(contract: dict) -> dict:
    return {p["name"]: p for p in contract["schema"][0]["properties"]}


def _unwrap(annotation):
    """Optional[X] / X | None / Annotated[X, ...] -> X."""
    origin = typing.get_origin(annotation)
    if origin is typing.Annotated:
        return _unwrap(typing.get_args(annotation)[0])
    if origin in (typing.Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return _unwrap(args[0])
    return annotation


def _sql_physical_type(col) -> str:
    """Normalized Postgres type name for a SQLAlchemy column, in the contract's spelling."""
    t = col.type
    if isinstance(t, JSONB):
        return "jsonb"
    if isinstance(t, UUID):
        return "uuid"
    if isinstance(t, Boolean):
        return "boolean"
    if isinstance(t, Float):
        return "double precision"
    if isinstance(t, Numeric):
        return f"numeric({t.precision},{t.scale})"
    if isinstance(t, DateTime):
        return "timestamptz" if t.timezone else "timestamp"
    if isinstance(t, Date):
        return "date"
    if isinstance(t, BigInteger):
        return "bigint"
    if isinstance(t, SmallInteger):
        return "smallint"
    if isinstance(t, Integer):
        return "integer"
    if isinstance(t, Text):
        return "text"
    if isinstance(t, String):
        return f"varchar({t.length})" if t.length else "varchar"
    return type(t).__name__.lower()


def _norm(physical_type: str) -> str:
    return physical_type.replace(" ", "").lower()


def _sql_default(col) -> bool:
    return col.default is not None or col.server_default is not None
