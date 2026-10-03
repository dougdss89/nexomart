# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data platform combining a FastAPI ingestion API (dbapi), Apache Airflow orchestration, Apache Spark distributed compute, Delta Lake and Iceberg tables (cataloged by the Hive Metastore, stored in MinIO), and Trino SQL federation — all connected via a shared Docker network (`hadoopcluster`).

## Commands

**Run API locally:**
```bash
cd dbapi
uv run python api/app/main.py
```

**Run a seeder:**
```bash
uv run python dbapi/api/seeder/<domain>/<file>.py --count 10000
```

**Run tests:**
```bash
cd dbapi
uv run python api/tests/test_product_services.py
```

**Start services — full stack (recommended):**
```bash
make setup        # first time only: creates network/volumes + builds images
make up           # brings up the full stack in dependency order
make down         # stops everything (data preserved)
make ps           # lists running containers
make logs s=spark-master        # tail logs for a container
make restart s=trino-coordinator  # restart a container
make clean        # ⚠️  removes containers + volumes (deletes data)
```

**Start services — individually:**
```bash
make up-postgres
make up-minio
make up-hive
make up-spark
make up-trino
make up-airflow
make up-dbapi
```

**URLs after `make up`:**
| Service | URL | Credentials |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:9090 | — |
| Spark History | http://localhost:18080 | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | admin / admin |

## Architecture

### 3-Layer API Architecture (dbapi)

All new models, schemas, and seeders **must** generate the full 3-layer stack:

1. **Repositories** (`dbapi/api/repositories/<domain>/`) — async data access only, no business logic. Use SQLAlchemy 2.0 bulk `insert(Model).values(matrix)`. **No for-loops.**
2. **Services** (`dbapi/api/services/<domain>/`) — business logic and orchestration. Import repositories, not raw SQL.
3. **Routers** (`dbapi/api/routers/<domain>_router.py`) — FastAPI endpoints with Pydantic schema validation.

### Models

All SQLAlchemy models in `dbapi/api/models/<domain>/` **must** inherit `AuditMixin` from `api/config/base.py` (adds `created_at`, `updated_at`, `is_active`, `row_guid`).

- Use `snake_case` for all PostgreSQL column names — never CamelCase
- Schema-qualified tables: `__table_args__ = ({"schema": "<domain>"})`
- Async driver: `postgresql+asyncpg://`

Domains: `product`, `sales`, `warehouse`, `purchasing`, `marketing`, `person`, `humanresources`

### Enums

Always inherit from `str`: `class Status(str, Enum)`. Enable `use_enum_values = True` in Pydantic `Config`.

### Pydantic

- Monetary fields: use `Decimal` with `Field(decimal_places=2, ge=0.0)` (`Numeric(19,4)` in DB)
- Cross-field validation: `@model_validator(mode='after')`

## Seeder Rules

Located in `dbapi/api/seeder/<domain>/`. Every seeder **must**:

- Implement `argparse` with `--count` argument (invoked by Airflow BashOperator via CLI)
- Fetch real foreign key IDs from the database — never generate orphaned records
- Use bulk insert: `insert(Model).values(matrix)` — **no for-loop inserts**
- Inject 5–10% chaos: null non-PK fields, malformed CPFs/CNPJs, "0000000" phones
- Print batch length and exceptions to stdout (for Airflow log visibility)
- Import sample/mock data from `dbapi/api/samples/<schema>.py`

For analytic fact seeders, derive revenue top-down: Impressions → CTR → Clicks → CPC → Cost → CVR → Conversions → AOV → Revenue.

### Data Volume Limits per Schema — Dynamic Ranges

**IMPORTANT**: Volumes are **dynamic and random** on every run of the Airflow DAG. Seeders must draw a value within the specified minimum/maximum range. Ranges are **per DAG run**, calibrated for `nexomart_seeder_dag` running every 10-15 minutes (~100 runs/day) — not per day. Single source of truth is `configs/seeders/config.py` (`SCHEMA_RANGES`/`TABLE_RANGES`); the table below mirrors it and must be kept in sync if those change. See `docs/SEEDER_MICROBATCHING.md` for why this is sized this way and how to rescale it if the schedule interval changes.

| Schema | Main Tables | Min Range | Max Range | Notes |
|--------|-------------------|--------------|--------------|-------------|
| `sales` | Orders, Returns | 1,000 | **50,000** | Per run — no absolute ceiling |
| `purchasing` | Purchase Orders (Header + Detail) | 1,000 | **50,000** | Combined header + detail volume, per run |
| `warehouse` | Transaction History, Inventory | 1,000 | **50,000** | Stock movements, per run |
| `humanresources` | Employees, Departments, JobRoles | 50 | **100** | HR is a small, stable dimension — not scaled by cadence |
| `product` | Products, Models, Categories | 500 | **1,500** | **Absolute limit 100k**. Seeder must check before inserting. Not scaled by cadence. |
| `person` | Customers, Addresses, Contacts | 1,000 | 50,000 | Based on sales volume (1:N), per run |
| `marketing` | Campaigns, Ads, Performance | 10 | 5,000 | Derived from SKUs and campaign periods, per run |

**Chaos Rate is also dynamic**: Randomly drawn between 5% and 8-10% on every run (per-schema/table max varies — see `configs/seeders/config.py`).

**Fan-out seeders** (`person_customer_email.py`, `purchase_purchaseorderdetail.py`, `purchase_purchaseorderaudit.py`) don't draw from a range here at all — their count is "however many parent rows (customers/headers) are still pending," found via a SQL anti-join and capped by the parent table's own `max_count` above as a per-run LIMIT. See `docs/SEEDER_MICROBATCHING.md`.

#### Implementation in Seeders

All seeders must accept **ranges** via argparse and draw a value within the range:

```python
import random

parser.add_argument("--min-count", type=int, default=100_000, help="Minimum record volume")
parser.add_argument("--max-count", type=int, default=50_000_000, help="Maximum record volume")
parser.add_argument("--min-chaos", type=float, default=0.01, help="Minimum chaos rate (1%)")
parser.add_argument("--max-chaos", type=float, default=0.10, help="Maximum chaos rate (10%)")

args = parser.parse_args()

# Draw dynamic values
count = random.randint(args.min_count, args.max_count)
chaos_rate = random.uniform(args.min_chaos, args.max_chaos)

print(f"🎲 Values drawn: {count:,} records | Chaos: {chaos_rate:.2%}")
```

**Absolute Limit Validation** (for `product`):

```python
# Before inserting products
existing_count = await session.scalar(select(func.count(ProductModel.product_id)))

# Absolute limit: 100k
ABSOLUTE_MAX = 100_000

if existing_count >= ABSOLUTE_MAX:
    print(f"⚠️ Absolute product limit reached ({existing_count}/{ABSOLUTE_MAX}).")
    return

# Adjust drawn count so it doesn't exceed the limit
count = min(count, ABSOLUTE_MAX - existing_count)
print(f"✅ Inserting {count:,} products (final total: {existing_count + count:,}/{ABSOLUTE_MAX:,})")
```

### Data Quality Rules — Chaos Engineering for Data Engineering

**Goal**: Simulate real-world data quality scenarios to test cleaning pipelines, fraud detection, and governance.

All seeders for the `sales`, `purchasing`, `warehouse`, `person` schemas must inject **between 1% and 10%** of the following anomalies:

#### 1. Data Quality Anomalies

- **Duplication** (1-5%): Duplicate records with small variations (case swaps, extra spaces, special characters)
  - Examples: `"João Silva"` vs `"JOÃO SILVA"` vs `"João  Silva "` (double space)
  - Duplicate CPF with different formatting: `"123.456.789-00"` vs `"12345678900"`

- **Dirty Data** (2-10%): Fields with malformed values that need regexp/normalization
  - Phones: `"0000000"`, `"11111111111"`, `"(00) 0000-0000"`
  - Postal codes: `"00000-000"`, `"XXXXX-XXX"`, postal codes from a different state than the address
  - CPF/CNPJ: `"000.000.000-00"`, `"111.111.111-11"`, with wrong check digits
  - Emails: `"email@domain"` (no TLD), `"@domain.com"` (no user), `"email@@domain.com"`
  - Names: `"John123"`, `"Mary###"`, `"Test Customer"`, `"XXXX"`, empty strings with whitespace `"   "`

- **Above-Normal Values** (1-3%): Statistical outliers (not fraudulent, just atypical)
  - Orders 5-10x above average (legitimate luxury purchases)
  - Very large but justifiable quantities (business purchase)
  - Very long delivery times (import shipments, freight strikes)

#### 2. Fraud and Suspicious Activity (Fraud Detection Scenarios)

- **Purchase Fraud** (1-2%):
  - Customer with a real name but fake CPF (`000.000.000-00`, sequences)
  - Customer with a fake name but a real address (deliveries to known addresses)
  - Multiple purchases with different cards at the same address in a short time window
  - Cloned cards: geographically impossible purchase pattern (purchase in São Paulo at 10am, in Rio de Janeiro at 10:05am)
  - High-value purchases with inconsistent registration data (free email + luxury address)

- **Fraudulent Registrations** (1-3%):
  - CPFs that are mathematically valid but don't actually exist (algorithm-generated)
  - Multiple registrations with the same phone/email but different CPFs
  - Fake or nonexistent addresses (Street X, number 99999, postal code invalid for the neighborhood)
  - Impossible combinations: Brazilian name + unregistered international phone number

- **Incomplete/Phantom Purchase Processes** (2-5%):
  - Orders created but never paid (`payment_status: "pending"` for more than 30 days)
  - Orders paid but never shipped (`shipping_status: null` or `"pending"` indefinitely)
  - Orders shipped but never delivered (tracking stalled for months)
  - Orders with products that don't exist in the catalog (reference to a deleted/nonexistent `product_id`)
  - Customer received the product but there's a chargeback dispute (`dispute_status: "open"`)

- **Purchasing Fraud** (1-2%):
  - Duplicate purchase orders for the same vendor (possible error or bribery)
  - Phantom vendors (valid CNPJ but a company with no economic activity)
  - Prices well above market rate (overbilling)
  - Orders approved by employees without authorization (employee_id lacking system permission)
  - Product deliveries to personal addresses instead of warehouses

#### 3. Warehouse Anomalies (Logistics and Inventory)

- **Lost/Phantom Transfers** (2-5%):
  - Transfer started (`status: "in_transit"`) but never completed (more than 90 days ago)
  - Transfer completed but received quantity differs from shipped quantity (breakage, theft)
  - Transfer between impossible locations (from warehouse A to A)
  - Transactions without traceability (`traceability_info: null` or incomplete)

- **Inventory Discrepancies** (3-8%):
  - Physical quantity differs from system quantity (needs cycle count)
  - Products with negative or zero cost (registration/system error)
  - Products marked as available but with no physical location
  - Negative stock (overselling)
  - Movements with no originating order (`reference_order_id: null` on an outbound transaction)

- **Quality Issues** (1-2%):
  - Products with `quality_check_status: "fail"` or `"hold"` but still shipped to customers
  - Products with IoT temperature readings outside the allowed range (broken cold chain)
  - Expired products still in active inventory

- **Negative Values** (2-4%):
  - Negative costs (system error, needs reversal)
  - Negative quantities in transactions (integration bug)
  - Negative freight values (poorly implemented system adjustment)

#### 4. Implementation in Seeders

Seeders must inject these anomalies in a **randomized** but **controlled** way:

```python
def apply_data_quality_chaos(batch: List[Dict], chaos_rate: float = 0.05) -> List[Dict]:
    """
    Applies chaos engineering (5% default) to the generated data.
    """
    num_chaos = int(len(batch) * chaos_rate)
    chaos_indices = random.sample(range(len(batch)), num_chaos)

    for idx in chaos_indices:
        chaos_type = random.choice([
            "duplicate", "dirty_data", "outlier", "fraud",
            "incomplete", "negative_values", "missing_refs"
        ])

        if chaos_type == "duplicate":
            # Duplicate the record with a variation
            batch.append({**batch[idx], "customer_name": batch[idx]["customer_name"].upper()})

        elif chaos_type == "dirty_data":
            # Inject a fake CPF
            batch[idx]["cpf"] = "000.000.000-00"

        elif chaos_type == "fraud":
            # Cloned card: purchases in impossible locations on the same day
            batch[idx]["shipping_city"] = "São Paulo"
            batch[idx]["billing_city"] = "Rio de Janeiro"
            batch[idx]["order_date"] = datetime.now()
            batch[idx]["payment_date"] = datetime.now() - timedelta(minutes=5)

        # ... other chaos types

    return batch
```

**CRITICAL NOTE**: This "dirty" data is **intentional** and must be **documented in metadata** to enable:
1. Testing cleaning pipelines (dbt, Spark)
2. Testing fraud detection (ML models)
3. Data quality audits (Great Expectations, Soda)
4. Training analysts on real-world scenarios

Create the file `dbapi/api/seeder/CHAOS_DOCUMENTATION.md` listing every type of anomaly injected, per table.

## Transactional ID Prefixes

| Domain | Header | Line Item |
|---|---|---|
| Sales | `SO-XXXXXXXXX` | `SO-XXXXXXXXX-XX` |
| Purchasing | `PO-XXXXXXXXX` | `PO-XXXXXXXXX-XX` |
| Warehouse | `TR-XXXXXXXXX` | `TR-XXXXXXXXX-XX` |
| Returns | `RO-XXXXXXXXX` | — |

9-digit random numeric sequences for `X`.

## Workflow: Creating New Pipelines / Analytical Areas

When asked to create a new pipeline (e.g. Sales, Logistics), **always** follow this hierarchy in order:

### Step 1 — SQLAlchemy Model (`dbapi/api/models/<domain>/`)
1. Create the model extending `Base` and `AuditMixin`.
2. Force `__tablename__` to lowercase.
3. JSONB indexes: declare with casting and map native `snake_case` keys.

### Step 2 — Pydantic Schema (`dbapi/api/schemas/<domain>/`)
1. Mirror the SQL model's keys `1:1`.
2. Enums must inherit from `str` with strict constraints.
3. Standard UUID and Booleans.
4. `@model_validator(mode='after')` for dates or business logic.

### Step 3 — Realistic Seeder (`dbapi/api/seeder/<domain>/`)
1. If it inherits data from another pipeline, import the array from the upstream seeder (e.g. `get_person_customer_seeders()`).
2. Cross-reference with satellite tables in `dbapi/api/samples/`.
3. Build the generator by injecting the raw JSON payload via `PipelineSchema(**payload).model_dump()` to shield the data.
4. Numeric facts (Time-Series): apply Funnel Math.
5. Entities (Dimensions): apply Chaos Engineering (5–10% controlled syntactic errors).

### Step 4 — REST 3-Layer Triad (Repository → Service → Router)
**Required** once the Seeder is done:
1. `_repository.py` in `dbapi/api/repositories/<domain>/` — async `bulk_insert()` via plain SQLAlchemy. **Zero for-loops.**
2. Abstract `_service.py` with analytical preconditions in `dbapi/api/services/<domain>/`.
3. Router in `dbapi/api/routers/<domain>_router.py` bound to `Depends(get_db)`.

### Step 5 — CLI / Airflow Integration
1. Expose `argparse` in the Seeder's `__main__` scope (e.g. `--count`, `--batch_size`) so it can be triggered via `BashOperator`.
2. Confirm no for-loop sends individual queries — all operations must be batched through the Triad.
3. Test in the terminal and report back to the user before declaring it "done".

---

## Infrastructure Configuration — Zero Hardcode

**Golden rule**: no connection string (host, port, database name, credential, service endpoint) may ever be written as a literal in `docker-compose.yml`/service configs. Everything comes from `configs/secrets.env` (the single source of truth — never committed, but mirrored in `secrets.env.example`). To add a new value: declare the variable in `secrets.env` **and** in `secrets.env.example`, never hardcode a new literal in any config file.

Three substitution mechanisms, depending on file type — use the right one for each:

1. **Docker Compose** (`*/docker-compose.yml`): native `${VAR}` syntax. The `Makefile` already exports `secrets.env` to the shell before any `docker compose up` (target `up`/`up-*`), so `${VAR}` inside the YAML resolves on its own. If the *container* also needs to see the variable at runtime (not just when the compose file is assembled), add `env_file: ../configs/secrets.env` to the service.
2. **Trino Catalogs** (`trino/catalog/*.properties`): Trino's own `${ENV:VAR}` syntax (not plain `${VAR}`). The `trino-coordinator`/`trino-worker` services already load `secrets.env` via `env_file`.
3. **Config files with no native variable support** (`spark-defaults.conf`, Hive's XML): **never edit the final file directly** — edit the matching `<file>.template` (uses shell-style `${VAR}`) and run `make generate-confs` (already included in `make up`), which runs `envsubst` and generates the real file. Edits to the generated file are lost on the next `generate-confs`.

Config files with no template and no variable support (`hive/conf/hive-site.xml`, `nessie/application.properties` — the latter isn't even mounted in any container, it's dead code) fall outside this pattern for now; don't hardcode into them unnecessarily, but also don't assume they accept `${VAR}` without testing.

---

## Key Files

- `dbapi/api/config/base.py` — `AuditMixin` and `Base` declarative base
- `dbapi/api/config/settings.py` — env-based configuration
- `dbapi/api/database/database.py` — `AsyncSessionLocal` factory
- `dbapi/api/samples/` — mock data templates imported by seeders
- `configs/.env` — shared env vars for PostgreSQL, MinIO, Spark, etc.
- `docs/walkthrough.md` — implementation walkthrough for the product domain (reference example)
