---
description: How to create new pipelines from scratch in the Data Platform while keeping the whole base architecture intact
---

# 🏗️ Blueprint: Creating New Analytical Areas (Schemas/Pipelines)

When the Engineer (USER) requests the creation of a new abstract Data Pipeline (e.g. Sales / `Sales`, Logistics / `Logistics`), you MUST follow exactly the hierarchy below:

### Step 1: The Database Model (`api/models/`)
1. Create the SQLAlchemy model.
2. Extend `Base` and `AuditMixin`.
3. Force `__tablename__` to lowercase.
4. JSONB indexes must be declared with casting and map the native `snake_case` keys.

### Step 2: The Pydantic Schema (`api/schemas/`)
1. Create properties mirroring `1:1` the Model's (SQL) keys.
2. Add strict constraints on string-extended Enums.
3. Use standard UUID and Booleans.
4. Implement a `model_validator` to gate dates or lock down corrupted business logic.

### Step 3: The Realistic Data Seeder (`api/seeder/`)
1. If this Pipeline inherits data from another one (e.g. Sales inherits from Customer), import the array generated in the upstream script (e.g. `get_person_customer_seeders()`).
2. Cross-reference the data with satellite tables (`samples/`).
3. Build the generator by injecting the raw JSON payload into the instantiated Pydantic Schema class (`PipelineSchema(**payload).model_dump()`) to shield the data.
4. If it's a numeric fact (Time-Series): apply Funnel Math.
5. If it's an entity (Dimension): apply `Chaos Engineering` (5-10% controlled syntactic errors).

### Step 4: The Architecture Triad (Repository, Service, Router)
**It is strictly mandatory** that, once the Seeder is generated, you proactively create the 3-Layer REST API base to go with the schema:
1. Create `_repository.py` under `api/repositories` to perform the async `bulk_insert()` via plain SQLAlchemy.
2. Create the abstract `_service.py` with the analytical preconditions.
3. Expose the Pydantic route in `api/routers` bound to `Depends(get_db)`.

### Step 5: CLI / Airflow Integration (Test Output)
1. Before declaring it "done", the Seeder script in the `__main__` scope MUST expose terminal parameters via `argparse` (e.g. `--count` or `--batch_size`) to enable autonomous triggering via Airflow's `BashOperator`.
2. Absolutely make sure no *for-loop* is sending individual queries to the database (all operations must go through the Triad as batches).
3. Test in the terminal and let the USER know.
