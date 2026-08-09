# Dynamic Randomization Guide — Unpredictable Seeders

This document explains how to use the seeders with **dynamic randomization** of volumes and chaos rates, making every Airflow DAG run unique and unpredictable.

## 🎯 Goal

Create a realistic data environment where:
- **Volumes vary** on every run (normal days vs demand spikes)
- **Chaos rates vary** on every run (1% to 10% anomalies)
- **Everything is configurable** via min/max ranges per schema
- **Fully integrated** with Airflow (BashOperator)

---

## 📦 Files Created

### 1. [api/config/seeder_config.py](../config/seeder_config.py)
**Centralized configuration of dynamic ranges**

Defines min/max ranges for each schema/table:

```python
from api.config.seeder_config import get_ranges

# Get ranges for sales
ranges = get_ranges(table="sales.salesorder")
print(ranges.min_count)  # 100_000
print(ranges.max_count)  # 50_000_000
print(ranges.min_chaos)  # 0.01
print(ranges.max_chaos)  # 0.10
```

**Configured ranges:**

| Schema | Min Volume | Max Volume | Min Chaos | Max Chaos |
|--------|------------|------------|-----------|-----------|
| sales | 100k | 50M | 1% | 10% |
| purchasing | 50k | 50M | 1% | 10% |
| warehouse | 100k | 50M | 2% | 10% |
| person | 10k | 5M | 3% | 10% |
| product | 500 | 1,500 | 2% | 5% |
| humanresources | 50 | 100 | 0% | 0% |
| marketing | 1k | 500k | 1% | 8% |

### 2. [sales/sales_order_EXAMPLE_WITH_CHAOS.py](./sales/sales_order_EXAMPLE_WITH_CHAOS.py)
**Example seeder with dynamic randomization**

Features:
- ✅ Imports ranges from `seeder_config.py`
- ✅ Accepts `--random` to randomize values
- ✅ Accepts custom `--min-count` / `--max-count`
- ✅ Accepts custom `--min-chaos` / `--max-chaos`
- ✅ Absolute limit validation (for product)
- ✅ Automatically recommended chaos types

**Usage:**

```bash
# 1. Random mode (recommended for Airflow)
uv run python sales_order_EXAMPLE_WITH_CHAOS.py --random

# 2. Custom ranges
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --min-count 100000 --max-count 1000000 \
    --min-chaos 0.01 --max-chaos 0.05 \
    --random

# 3. Fixed values (development)
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --count 1000 --chaos-rate 0.05

# 4. No chaos
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --count 1000 --no-chaos
```

### 3. [airflow/dags/EXAMPLE_seeder_dag_with_randomization.py](../../airflow/dags/EXAMPLE_seeder_dag_with_randomization.py)
**Airflow DAG with randomization**

Features:
- ✅ Runs every seeder with `--random`
- ✅ Respects dependencies (dimensions → facts → analytics)
- ✅ Parallel execution when possible
- ✅ Detailed logs of the values drawn
- ✅ Retries and alerts configured

**Schedule:** Daily at 02:00

---

## 🚀 How It Works

### Randomization Flow

```
┌─────────────────────────────────────┐
│  Airflow DAG (daily at 02:00)       │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  BashOperator calls the seeder with:│
│  --random --min-count X --max-count Y│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Seeder draws values:               │
│  count = random.randint(min, max)   │
│  chaos = random.uniform(min, max)   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Generates clean data (count records)│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Applies chaos engineering (chaos %)│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Bulk inserts into the database     │
└─────────────────────────────────────┘
```

### Example Run

**Run 1 (normal day):**
```
🎲 RANDOM MODE ENABLED
   📊 Volume range: 100,000 to 50,000,000
   📊 Chaos range: 1.00% to 10.00%

   🎯 Values drawn:
      - Volume: 150,000 records
      - Chaos rate: 3.45%

📣 Seeding 150,000 sales orders
   - Batch size: 10,000
   - Chaos rate: 3.45%
   - Existing records: 0

   ⏳ Progress: 10,000/150,000 (6.7%)
   ⏳ Progress: 20,000/150,000 (13.3%)
   ...
   ⏳ Progress: 150,000/150,000 (100.0%)

✅ Seeder finished!
   - Records inserted: 150,000
   - Total in table: 150,000
   - Anomalies injected: ~5,175 (3.45%)
```

**Run 2 (simulated Black Friday):**
```
🎲 RANDOM MODE ENABLED
   🎯 Values drawn:
      - Volume: 35,000,000 records
      - Chaos rate: 8.92%

✅ Seeder finished!
   - Records inserted: 35,000,000
   - Total in table: 35,150,000
   - Anomalies injected: ~3,122,000 (8.92%)
```

---

## 🔧 Migrating Existing Seeders

### Step 1: Import Configuration

At the top of the seeder:

```python
from api.config.seeder_config import get_ranges, get_recommended_chaos_types

# Get configured ranges
SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"
RANGES = get_ranges(table=TABLE_NAME)
```

### Step 2: Add CLI Arguments

```python
parser.add_argument(
    "--random",
    action="store_true",
    help="Draws values within the configured ranges"
)
parser.add_argument(
    "--min-count",
    type=int,
    default=RANGES.min_count,
    help=f"Minimum volume (default: {RANGES.min_count:,})"
)
parser.add_argument(
    "--max-count",
    type=int,
    default=RANGES.max_count,
    help=f"Maximum volume (default: {RANGES.max_count:,})"
)
parser.add_argument(
    "--min-chaos",
    type=float,
    default=RANGES.min_chaos,
    help=f"Minimum chaos rate (default: {RANGES.min_chaos:.2%})"
)
parser.add_argument(
    "--max-chaos",
    type=float,
    default=RANGES.max_chaos,
    help=f"Maximum chaos rate (default: {RANGES.max_chaos:.2%})"
)
```

### Step 3: Implement the Randomization Logic

```python
if args.random:
    # RANDOM MODE: draw values
    count = random.randint(args.min_count, args.max_count)
    chaos_rate = random.uniform(args.min_chaos, args.max_chaos) if not args.no_chaos else 0.0

    print("🎲 RANDOM MODE ENABLED")
    print(f"   🎯 Values drawn:")
    print(f"      - Volume: {count:,} records")
    print(f"      - Chaos rate: {chaos_rate:.2%}")
else:
    # FIXED MODE: use the specified values
    count = args.count
    chaos_rate = args.chaos_rate
```

### Step 4: Use the Recommended Chaos Types

```python
# Automatically get the recommended chaos types
chaos_types = get_recommended_chaos_types(SCHEMA_NAME)

# Apply chaos
if apply_chaos and chaos_types:
    orders_with_chaos = apply_data_quality_chaos(
        orders,
        chaos_rate=chaos_rate,
        allowed_chaos_types=chaos_types,
        verbose=False
    )
```

---

## 📋 Migration Checklist

For each seeder:

- [ ] Import `get_ranges` and `get_recommended_chaos_types`
- [ ] Define `SCHEMA_NAME` and `TABLE_NAME`
- [ ] Get `RANGES = get_ranges(table=TABLE_NAME)`
- [ ] Add CLI arguments (`--random`, `--min-count`, etc.)
- [ ] Implement the randomization logic (`if args.random:`)
- [ ] Use `get_recommended_chaos_types(SCHEMA_NAME)`
- [ ] Test with `--random`
- [ ] Test with fixed values (`--count`)
- [ ] Add to the Airflow DAG

---

## 🎲 Full Migration Example

### Before (old seeder):
```python
parser.add_argument("--count", type=int, default=1000)

count = args.count
orders = generate_sales_order_payloads(count, ...)
stmt = insert(SalesOrderModel).values(orders)
```

### After (seeder with randomization):
```python
from api.config.seeder_config import get_ranges, get_recommended_chaos_types

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"
RANGES = get_ranges(table=TABLE_NAME)

parser.add_argument("--random", action="store_true")
parser.add_argument("--min-count", type=int, default=RANGES.min_count)
parser.add_argument("--max-count", type=int, default=RANGES.max_count)
parser.add_argument("--min-chaos", type=float, default=RANGES.min_chaos)
parser.add_argument("--max-chaos", type=float, default=RANGES.max_chaos)

if args.random:
    count = random.randint(args.min_count, args.max_count)
    chaos_rate = random.uniform(args.min_chaos, args.max_chaos)
    print(f"🎲 Drawn: {count:,} records | Chaos: {chaos_rate:.2%}")
else:
    count = args.count
    chaos_rate = args.chaos_rate

orders = generate_sales_order_payloads(count, ...)
chaos_types = get_recommended_chaos_types(SCHEMA_NAME)
orders_with_chaos = apply_data_quality_chaos(orders, chaos_rate, chaos_types)
stmt = insert(SalesOrderModel).values(orders_with_chaos)
```

---

## 🐛 Troubleshooting

### Problem 1: Values aren't being randomized
**Cause**: Forgot to pass `--random` on the CLI or in the DAG

**Solution**:
```bash
# ❌ Wrong (uses fixed values)
python seeder.py --count 1000

# ✅ Correct (randomizes)
python seeder.py --random
```

### Problem 2: Chaos rate is always zero
**Cause**: `--no-chaos` is active, or the min/max range is 0.0

**Solution**:
```bash
# Check ranges
python -c "from api.config.seeder_config import get_ranges; print(get_ranges('sales'))"

# Force chaos
python seeder.py --random --min-chaos 0.05 --max-chaos 0.10
```

### Problem 3: Volume exceeded the absolute limit (product)
**Cause**: Not validating `absolute_max`

**Solution**:
```python
if RANGES.absolute_max:
    existing = await session.scalar(select(func.count(ProductModel.product_id)))
    count = min(count, RANGES.absolute_max - existing)
```

### Problem 4: Airflow DAG isn't randomizing
**Cause**: BashOperator command missing `--random`

**Solution**:
```python
# ❌ Wrong
bash_command="python seeder.py --count 1000"

# ✅ Correct
bash_command=f"python seeder.py --random --min-count {min} --max-count {max}"
```

---

## 📊 Monitoring and Auditing

### Important Logs

Each seeder should produce logs with:

```
🎲 RANDOM MODE ENABLED
   📊 Volume range: 100,000 to 50,000,000
   📊 Chaos range: 1.00% to 10.00%

   🎯 Values drawn:
      - Volume: 1,234,567 records
      - Chaos rate: 5.67%

📣 Seeding 1,234,567 sales orders (Fact Table)
   - Batch size: 10,000
   - Chaos rate: 5.67%
   - Existing records: 0

   ⏳ Progress: 10,000/1,234,567 (0.8%)
   ...

✅ Seeder finished!
   - Records inserted: 1,234,567
   - Total in table: 1,234,567
   - Anomalies injected: ~70,000 (5.67%)
```

### Audit Queries

```sql
-- Total records per table
SELECT
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname IN ('sales', 'purchasing', 'warehouse', 'person')
ORDER BY n_live_tup DESC;

-- Anomaly distribution (example: invalid CPFs)
SELECT
    'Invalid CPF' as anomaly_type,
    COUNT(*) as count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person.customer) as percentage
FROM person.customer
WHERE cpf IN ('000.000.000-00', '111.111.111-11', '123.456.789-00');
```

---

## 🔄 Customizing Ranges

### By Schema

Edit [`api/config/seeder_config.py`](../config/seeder_config.py):

```python
SCHEMA_RANGES = {
    "sales": SchemaRanges(
        min_count=100_000,      # ← Adjust here
        max_count=50_000_000,   # ← Adjust here
        min_chaos=0.01,         # ← Adjust here
        max_chaos=0.10,         # ← Adjust here
        description="..."
    ),
}
```

### By Specific Table

```python
TABLE_RANGES = {
    "sales.salesorder": SchemaRanges(
        min_count=200_000,      # Override for this table
        max_count=60_000_000,
        min_chaos=0.02,
        max_chaos=0.12,
        description="..."
    ),
}
```

### Via CLI (Temporary Override)

```bash
# Force specific ranges for a single run
python seeder.py --random \
    --min-count 500000 \
    --max-count 1000000 \
    --min-chaos 0.05 \
    --max-chaos 0.08
```

---

## 📚 References

- **Range Configuration**: [api/config/seeder_config.py](../config/seeder_config.py)
- **Example Seeder**: [sales/sales_order_EXAMPLE_WITH_CHAOS.py](./sales/sales_order_EXAMPLE_WITH_CHAOS.py)
- **Example DAG**: [airflow/dags/EXAMPLE_seeder_dag_with_randomization.py](../../airflow/dags/EXAMPLE_seeder_dag_with_randomization.py)
- **Chaos Engineering**: [CHAOS_DOCUMENTATION.md](./CHAOS_DOCUMENTATION.md)
- **General Migration**: [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)
- **General Rules**: [CLAUDE.md](../../CLAUDE.md)

---

## ✅ Conclusion

With dynamic randomization, every Airflow DAG run is **unique and unpredictable**, simulating realistic production scenarios:

- 📊 **Normal days**: 100k records, 1% chaos
- 🔥 **Demand spikes**: 50M records, 10% chaos
- 🎲 **Fully random**: different values on every run

This enables:
- Testing cleaning pipelines with varied volumes
- Training fraud detection models with different anomaly rates
- Validating system scalability (normal days vs spikes)
- Simulating realistic production environments
