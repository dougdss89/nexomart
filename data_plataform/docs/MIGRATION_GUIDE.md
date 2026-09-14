# Migration Guide — Seeders with Chaos Engineering

This document explains how to migrate existing seeders to the new pattern with chaos engineering and volume limits.

## Summary of Changes

### 1. New Files Created

| File | Description |
|---------|-----------|
| [`CHAOS_DOCUMENTATION.md`](./CHAOS_DOCUMENTATION.md) | Full documentation of every injected anomaly |
| [`api/utils/chaos_engineering.py`](../utils/chaos_engineering.py) | Utility module with chaos engineering functions |
| [`sales/sales_order_EXAMPLE_WITH_CHAOS.py`](./sales/sales_order_EXAMPLE_WITH_CHAOS.py) | Example seeder with the new rules applied |

### 2. Updates to CLAUDE.md

The [`CLAUDE.md`](../../CLAUDE.md) file was updated with:
- **Volume limits per schema** (section "Data Volume Limits per Schema")
- **Data quality rules** (section "Data Quality Rules — Chaos Engineering")
- **Chaos engineering implementation** in the seeders

## Required Changes to Existing Seeders

### Step 1: Add Limit Validation

For seeders in the `product` schema, add validation **before** inserting:

```python
from sqlalchemy import func

# Example: product seeder
MAX_PRODUCTS = 100_000

existing_count = await session.scalar(select(func.count(ProductModel.product_id)))
if existing_count + count > MAX_PRODUCTS:
    print(f"⚠️ Product limit reached ({existing_count}/{MAX_PRODUCTS}).")
    print(f"   Only {MAX_PRODUCTS - existing_count} more records can be inserted.")
    return
```

### Step 2: Import the Chaos Engineering Module

Add at the top of the file:

```python
from api.utils.chaos_engineering import apply_data_quality_chaos, generate_chaos_metadata
```

### Step 3: Apply Chaos Engineering Before Insertion

**BEFORE** (old code):
```python
orders = generate_sales_order_payloads(count, customer_ids, ...)

stmt = insert(SalesOrderModel).values(orders)
await session.execute(stmt)
```

**AFTER** (new code):
```python
# 1. Generate clean data
orders = generate_sales_order_payloads(count, customer_ids, ...)

# 2. Apply chaos engineering (5% anomalies)
orders_with_chaos = apply_data_quality_chaos(
    orders,
    chaos_rate=0.05,
    allowed_chaos_types=[
        "duplicate", "fraud_fake_cpf", "incomplete_payment"
    ],
    verbose=True
)

# 3. Insert with anomalies
stmt = insert(SalesOrderModel).values(orders_with_chaos)
await session.execute(stmt)
```

### Step 4: Add CLI Arguments

Update `argparse` to allow controlling the chaos rate:

```python
parser.add_argument("--chaos-rate", type=float, default=0.05, help="Anomaly percentage (0.01 to 0.10)")
parser.add_argument("--no-chaos", action="store_true", help="Disables chaos engineering")
```

### Step 5: Generate Metadata (Optional)

For documentation purposes, generate chaos metadata:

```python
if apply_chaos:
    metadata = generate_chaos_metadata(
        table_name="sales.salesorder",
        chaos_rate=chaos_rate,
        chaos_types=["duplicate", "fraud_fake_cpf", ...]
    )
    print(f"\n📊 Chaos Metadata: {metadata}")
```

## Chaos Types by Schema

### Sales

Recommended chaos types:
- `duplicate` — Duplicate records
- `dirty_cpf` — Invalid CPF
- `dirty_email` — Malformed email
- `fraud_fake_cpf` — Fake CPF with a real name
- `fraud_cloned_card` — Geographically impossible purchases
- `fraud_high_value_suspicious` — High value with suspicious data
- `incomplete_payment` — Payment pending > 30 days
- `incomplete_shipping` — Paid but not shipped > 15 days
- `incomplete_delivery` — Shipped but not delivered > 90 days

### Purchasing

Recommended chaos types:
- `duplicate` — Duplicate records
- `fraud_duplicate_order` — Duplicate orders for the same vendor
- `fraud_phantom_vendor` — Phantom vendors
- `fraud_overpriced` — Overpricing (3-8x above market)
- `negative_cost` — Negative costs
- `missing_vendor_ref` — Purchase without a vendor

### Warehouse

Recommended chaos types:
- `warehouse_lost_transfer` — Lost transfers (> 90 days)
- `warehouse_quantity_mismatch` — Quantity mismatch
- `warehouse_negative_stock` — Negative stock
- `warehouse_missing_location` — Product without a physical location
- `warehouse_quality_fail` — Product failed QC but still shipped
- `warehouse_cold_chain_break` — Broken cold chain
- `warehouse_same_location_transfer` — Transfer to the same location
- `warehouse_missing_traceability` — Missing traceability
- `missing_order_ref` — Movement without a source order

### Person

Recommended chaos types:
- `duplicate` — Duplicate records
- `dirty_cpf` — Invalid CPF
- `dirty_phone` — Invalid phone
- `dirty_email` — Malformed email
- `dirty_cep` — Invalid postal code (CEP)
- `dirty_name` — Invalid name
- `fraud_fake_address` — Fake name with a real address

### HumanResources

**Do not apply chaos engineering** — Volume is too low (100 rows) and the data is critical for HR.

### Product

**Apply moderate chaos** (2-3%) — Controlled volume (max 100k):
- `duplicate` — Duplicate products
- `outlier_value` — Prices way too high/low

## Full Migration Example

See [`sales/sales_order_EXAMPLE_WITH_CHAOS.py`](./sales/sales_order_EXAMPLE_WITH_CHAOS.py) for a full example of a migrated seeder.

## Recommended Tests

After migrating a seeder, run tests to validate:

1. **Test with clean data** (no chaos):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --no-chaos
   ```

2. **Test with low chaos** (1%):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --chaos-rate 0.01
   ```

3. **Test with high chaos** (10%):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --chaos-rate 0.10
   ```

4. **Verify injected anomalies**:
   ```sql
   -- Invalid CPFs
   SELECT COUNT(*) FROM person.customer WHERE cpf IN ('000.000.000-00', '111.111.111-11');

   -- Duplicates (case-insensitive)
   SELECT UPPER(customer_name), COUNT(*) FROM person.customer GROUP BY UPPER(customer_name) HAVING COUNT(*) > 1;

   -- Negative stock
   SELECT COUNT(*) FROM warehouse.product_inventory WHERE quantity_on_hand < 0;
   ```

## Migration Priority

### High Priority (Critical Schemas)

1. **`sales/sales_order.py`** — Main sales fact
2. **`purchasing/purchase_purchaseorderheader.py`** — Main purchasing fact
3. **`warehouse/transaction_history.py`** — Main movement fact
4. **`person/person_customer.py`** — Critical customer dimension

### Medium Priority

5. **`sales/sales_return_order.py`** — Returns (important for quality analysis)
6. **`warehouse/product_inventory.py`** — Current stock
7. **`purchasing/purchase_purchaseorderdetail.py`** — Purchase details

### Low Priority

8. Stable dimension seeders (marketing, product categories, etc.)
9. Lookup table seeders (channels, transaction types, etc.)

## Migration Checklist

For each migrated seeder:

- [ ] Add limit validation (if applicable)
- [ ] Import `apply_data_quality_chaos` and `generate_chaos_metadata`
- [ ] Apply chaos engineering before insertion
- [ ] Add CLI arguments (`--chaos-rate`, `--no-chaos`)
- [ ] Choose chaos types appropriate to the domain
- [ ] Test with clean data and with chaos
- [ ] Verify anomalies injected into the database
- [ ] Update documentation (code comments)
- [ ] Generate chaos metadata (optional)

## Next Steps

1. **Migrate priority seeders** (sales, purchasing, warehouse, person)
2. **Create data quality dashboards** (Trino + analytical queries)
3. **Implement cleaning pipelines** (dbt models)
4. **Train fraud detection models** (ML)
5. **Integrate with validation tools** (Great Expectations, Soda)

## FAQ

### 1. Why intentionally inject bad data?

To simulate real-world data engineering scenarios and test cleaning pipelines, fraud detection, and governance.

### 2. Can I disable chaos engineering?

Yes, use `--no-chaos` to generate clean data only (useful for tests or production environments that don't need chaos).

### 3. How do I choose the ideal chaos_rate?

- **1-3%**: Production environments or final validation
- **5%** (default): Development and pipeline testing
- **10%**: Stress testing cleaning systems

### 4. How do I document the injected anomalies?

Use `generate_chaos_metadata()` and save it to a JSON file or a metadata database.

### 5. How do I validate that chaos was applied correctly?

Run detection queries (see the "Recommended Tests" section) and check that the counts are close to the expected chaos_rate.

---

**Documentation**: For more details on anomaly types, see [`CHAOS_DOCUMENTATION.md`](./CHAOS_DOCUMENTATION.md).
