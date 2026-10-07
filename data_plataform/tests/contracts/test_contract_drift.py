"""Drift check: api data contract (YAML) vs Pydantic schema vs SQLAlchemy model.

The Pydantic schema enforces the payload at runtime and the YAML documents it. Both are
written by hand, so this test fails when they stop agreeing with each other or with the
model. To cover another table, add a row to REGISTRY in contract_helpers.py. The model vs
the live database is checked separately in test_db_drift.py.
"""

import enum
import re
import typing
from decimal import Decimal

import pytest
from annotated_types import Ge, Gt, MaxLen
from pydantic import BaseModel
from contract_helpers import (
    DB_ONLY_COLUMNS,
    LOGICAL_TO_PYTHON,
    REGISTRY,
    _import,
    _load,
    _norm,
    _props,
    _sql_default,
    _sql_physical_type,
    _unwrap,
)


@pytest.fixture(params=REGISTRY, ids=[r[0] for r in REGISTRY])
def bundle(request):
    contract_file, model_path = request.param
    contract, schema = _load(contract_file)
    table = _import(model_path).__table__
    return contract, _props(contract), schema, table


def test_contract_points_to_the_real_table(bundle):
    contract, _, _, table = bundle
    assert contract["schema"][0]["physicalName"] == f"{table.schema}.{table.name}"


def test_field_names_match_pydantic(bundle):
    _, props, schema, _ = bundle
    contract_only = set(props) - set(schema.model_fields)
    pydantic_only = set(schema.model_fields) - set(props)
    assert not contract_only and not pydantic_only, (
        f"in contract but not in Pydantic: {sorted(contract_only)}; "
        f"in Pydantic but not in contract: {sorted(pydantic_only)}"
    )


def test_field_names_match_db_columns(bundle):
    _, props, _, table = bundle
    columns = {c.name for c in table.columns} - DB_ONLY_COLUMNS
    contract_only = set(props) - columns
    db_only = columns - set(props)
    assert not contract_only and not db_only, (
        f"in contract but not in the table: {sorted(contract_only)}; "
        f"in the table but not in contract: {sorted(db_only)}"
    )


def test_required_flag_matches_pydantic(bundle):
    _, props, schema, _ = bundle
    wrong = {
        name: (prop.get("required", False), schema.model_fields[name].is_required())
        for name, prop in props.items()
        if name in schema.model_fields
        and prop.get("required", False) != schema.model_fields[name].is_required()
    }
    assert not wrong, f"required mismatch (contract, pydantic): {wrong}"


def test_not_null_columns_are_required_in_contract(bundle):
    _, props, _, table = bundle
    missing = [
        c.name
        for c in table.columns
        if c.name in props
        and not c.nullable
        and not _sql_default(c)
        and not (c.primary_key and c.autoincrement is True)
        and not props[c.name].get("required", False)
    ]
    assert not missing, f"NOT NULL without default, but not required in contract: {missing}"


def test_physical_type_matches_db(bundle):
    _, props, _, table = bundle
    wrong = {
        c.name: (props[c.name]["physicalType"], _sql_physical_type(c))
        for c in table.columns
        if c.name in props and _norm(props[c.name]["physicalType"]) != _sql_physical_type(c)
    }
    assert not wrong, f"physicalType mismatch (contract, table): {wrong}"


def test_max_length_matches_pydantic(bundle):
    _, props, schema, _ = bundle
    wrong = {}
    for name, prop in props.items():
        m = re.fullmatch(r"varchar\((\d+)\)", _norm(prop["physicalType"]))
        if not m or name not in schema.model_fields:
            continue
        annotation = _unwrap(schema.model_fields[name].annotation)
        if isinstance(annotation, type) and issubclass(annotation, enum.Enum):
            continue  # an enum already bounds the length
        declared = int(m.group(1))
        pydantic_max = next(
            (x.max_length for x in schema.model_fields[name].metadata if isinstance(x, MaxLen)), None
        )
        if pydantic_max != declared:
            wrong[name] = (declared, pydantic_max)
    assert not wrong, f"max_length mismatch (contract, pydantic): {wrong}"


def test_logical_type_matches_pydantic(bundle):
    _, props, schema, _ = bundle
    wrong = {}
    for name, prop in props.items():
        if name not in schema.model_fields:
            continue
        annotation = _unwrap(schema.model_fields[name].annotation)
        allowed = LOGICAL_TO_PYTHON[prop["logicalType"]]
        ok = isinstance(annotation, type) and issubclass(annotation, allowed)
        if not ok:
            wrong[name] = (prop["logicalType"], getattr(annotation, "__name__", str(annotation)))
    assert not wrong, f"logicalType vs Pydantic annotation (contract, pydantic): {wrong}"


def test_numeric_rules_match_pydantic(bundle):
    _, props, schema, _ = bundle
    wrong = {}
    for name, field in schema.model_fields.items():
        if name not in props:
            continue
        pydantic_rules = {}
        for m in field.metadata:
            if isinstance(m, Gt):
                pydantic_rules["mustBeGreaterThan"] = m.gt
            elif isinstance(m, Ge):
                pydantic_rules["mustBeGreaterOrEqualTo"] = m.ge
        contract_rules = {}
        for rule in props[name].get("quality", []):
            for key in ("mustBeGreaterThan", "mustBeGreaterOrEqualTo"):
                if key in rule:
                    contract_rules[key] = rule[key]
        if pydantic_rules != contract_rules:
            wrong[name] = {"contract": contract_rules, "pydantic": pydantic_rules}
    assert not wrong, f"numeric rules mismatch: {wrong}"


def test_enum_examples_match_pydantic(bundle):
    _, props, schema, _ = bundle
    wrong = {}
    for name, field in schema.model_fields.items():
        annotation = _unwrap(field.annotation)
        if name in props and isinstance(annotation, type) and issubclass(annotation, enum.Enum):
            values = {e.value for e in annotation}
            examples = set(props[name].get("examples", []))
            if examples != values:
                wrong[name] = {"contract examples": sorted(examples), "enum": sorted(values)}
    assert not wrong, f"enum values vs contract examples: {wrong}"


def _logical_ok(logical, annotation) -> bool:
    if annotation is bool:
        return logical == "boolean"
    allowed = LOGICAL_TO_PYTHON.get(logical)
    return bool(allowed) and isinstance(annotation, type) and issubclass(annotation, allowed)


def _nested_problems(props: list, model, path: str) -> list:
    """Compares a nested contract object (a jsonb column's inner shape) with its Pydantic sub-model,
    recursively: names, required, logical type, string size, numeric bounds, decimal places, enums."""
    problems = []
    fields = model.model_fields
    listed = {p["name"] for p in props}
    for n in sorted(listed - set(fields)):
        problems.append(f"{path}.{n}: in contract but not in Pydantic")
    for n in sorted(set(fields) - listed):
        problems.append(f"{path}.{n}: in Pydantic but not in contract")

    for prop in props:
        name = prop["name"]
        if name not in fields:
            continue
        field = fields[name]
        here = f"{path}.{name}"

        if prop.get("required", False) != field.is_required():
            problems.append(
                f"{here}: required is {prop.get('required', False)} in contract, {field.is_required()} in Pydantic"
            )

        annotation = _unwrap(field.annotation)

        if typing.get_origin(annotation) is list:
            inner = _unwrap(typing.get_args(annotation)[0])
            items = prop.get("items", {})
            if prop["logicalType"] != "array":
                problems.append(f"{here}: Pydantic has a list, contract logicalType is {prop['logicalType']}")
            if isinstance(inner, type) and issubclass(inner, BaseModel):
                problems += _nested_problems(items.get("properties", []), inner, here + "[]")
            elif not _logical_ok(items.get("logicalType"), inner):
                problems.append(f"{here}[]: item logicalType {items.get('logicalType')} does not fit {inner}")
            continue

        if not _logical_ok(prop["logicalType"], annotation):
            problems.append(
                f"{here}: logicalType {prop['logicalType']} does not fit Pydantic type "
                f"{getattr(annotation, '__name__', annotation)}"
            )

        is_enum = isinstance(annotation, type) and issubclass(annotation, enum.Enum)
        opts = prop.get("logicalTypeOptions", {})
        metadata = field.metadata

        pyd_max = next((m.max_length for m in metadata if isinstance(m, MaxLen)), None)
        if not is_enum and opts.get("maxLength") != pyd_max:
            problems.append(f"{here}: maxLength is {opts.get('maxLength')} in contract, {pyd_max} in Pydantic")

        pyd_ge = next((m.ge for m in metadata if isinstance(m, Ge)), None)
        pyd_gt = next((m.gt for m in metadata if isinstance(m, Gt)), None)
        if opts.get("minimum") != pyd_ge or opts.get("exclusiveMinimum") != pyd_gt:
            problems.append(
                f"{here}: lower bound is (minimum={opts.get('minimum')}, exclusiveMinimum={opts.get('exclusiveMinimum')}) "
                f"in contract, (ge={pyd_ge}, gt={pyd_gt}) in Pydantic"
            )

        pyd_places = next(
            (getattr(m, "decimal_places") for m in metadata if getattr(m, "decimal_places", None) is not None), None
        )
        contract_places = (
            -Decimal(str(opts["multipleOf"])).as_tuple().exponent if "multipleOf" in opts else None
        )
        if contract_places != pyd_places:
            problems.append(f"{here}: decimal places are {contract_places} in contract, {pyd_places} in Pydantic")

        if is_enum:
            values, examples = {e.value for e in annotation}, set(prop.get("examples", []))
            if values != examples:
                problems.append(f"{here}: enum values {sorted(values)} vs contract examples {sorted(examples)}")

        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            problems += _nested_problems(prop.get("properties", []), annotation, here)
    return problems


def test_nested_objects_match_pydantic(bundle):
    """demographics is one jsonb column, so neither the model nor the database checks what is
    inside it. This is the only guard for its inner rules."""
    _, props, schema, _ = bundle
    problems = []
    for name, prop in props.items():
        if name not in schema.model_fields:
            continue
        annotation = _unwrap(schema.model_fields[name].annotation)
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            if "properties" not in prop:
                problems.append(f"{name}: is a Pydantic model but the contract lists no nested properties")
            else:
                problems += _nested_problems(prop["properties"], annotation, name)
    assert not problems, "\n" + "\n".join(problems)
