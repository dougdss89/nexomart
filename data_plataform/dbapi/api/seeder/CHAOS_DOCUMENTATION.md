# Chaos Engineering Documentation — Data Quality Anomalies

This document describes every anomaly intentionally injected into the data by the seeders to simulate real-world data engineering scenarios.

## Goal

Create a realistic test environment that enables:
1. Developing and testing data cleaning pipelines
2. Training fraud detection models
3. Validating data quality rules
4. Preparing analysts for real production scenarios

## Anomaly Rate

All seeders in the `sales`, `purchasing`, `warehouse`, `person`, `humanresources`, `marketing` schemas inject **between 1% and 10%** anomalies, distributed as specified below.

---

## 1. Data Quality Anomalies

### 1.1 Duplication (1-5%)

**Description**: Duplicate records with small variations that make automatic detection harder.

**Affected Tables**: `person.customer`, `sales.salesorder`, `purchasing.purchaseorderheader`

**Duplication Types**:

| Type | Original Example | Duplicated Example | Technique |
|------|------------------|-------------------|---------|
| Case sensitivity | `"João Silva"` | `"JOÃO SILVA"` | Upper/lowercase |
| Spacing | `"Maria Santos"` | `"Maria  Santos "` | Extra spaces, leading/trailing |
| CPF formatting | `"123.456.789-00"` | `"12345678900"` | With/without formatting |
| Special characters | `"José"` | `"Jose"` | Accent removed |
| Name order | `"Silva, João"` | `"João Silva"` | Order reversed |

**Test Scenarios**:
- Fuzzy matching with Levenshtein distance
- String normalization (unaccent, trim, lowercase)
- Duplicate detection via embeddings
- Merging duplicate records by choosing a golden record

---

### 1.2 Dirty Data (2-10%)

**Description**: Fields with malformed values that require regex, normalization, or enrichment.

#### 1.2.1 Invalid Phone Numbers

**Tables**: `person.customer`, `person.customer_email`, `purchasing.vendor`

| Invalid Pattern | Error Type | Expected Handling |
|-----------------|--------------|---------------------|
| `"0000000"` | Placeholder | Flag as invalid, request re-entry |
| `"11111111111"` | Sequence | Validate for unique digits |
| `"(00) 0000-0000"` | Format with no real number | Validate against a list of valid area codes |
| `"99 99999-9999"` | Non-existent area code | Validate area code against the ANATEL table |
| `"+55 11 91234-567"` | Missing digits | Validate length (landline: 10, mobile: 11) |

#### 1.2.2 Invalid Postal Codes (CEP)

**Tables**: `person.address`, `warehouse.warehouselocation`

| Invalid Pattern | Error Type | Expected Handling |
|-----------------|--------------|---------------------|
| `"00000-000"` | Placeholder | Enrich via ViaCEP API or similar |
| `"XXXXX-XXX"` | Non-numeric | Validate format and existence |
| `"01310-100"` (SP) + `state: "RJ"` | Geographic inconsistency | Cross-check CEP against expected state |
| `"12345-678"` | Non-existent CEP | Validate against the postal service database |

#### 1.2.3 Invalid CPF/CNPJ

**Tables**: `person.customer`, `purchasing.vendor`, `humanresources.employee`

| Invalid Pattern | Error Type | Expected Handling |
|-----------------|--------------|---------------------|
| `"000.000.000-00"` | Placeholder | Flag as invalid |
| `"111.111.111-11"` | Sequence | Block repeated sequences |
| `"123.456.789-09"` | Wrong check digit | Recompute and correct, or invalidate |
| `"12345678901234"` (14 digits, no formatting) | CPF/CNPJ confusion | Detect length and format correctly |
| Mathematically valid CPF but not registered with the tax authority | Synthetic CPF | Enrich against the tax authority database (if available) |

#### 1.2.4 Invalid Emails

**Tables**: `person.customer_email`, `humanresources.employee`

| Invalid Pattern | Error Type | Expected Handling |
|-----------------|--------------|---------------------|
| `"email@domain"` | Missing TLD | Validate RFC 5322 format |
| `"@domain.com"` | Missing user | Reject or request correction |
| `"email@@domain.com"` | Duplicated characters | Normalize before validating |
| `"email@domain .com"` | Invalid spaces | Trim and validate |
| `"test@test.test"` | Test email | Flag as suspicious (possible fraud) |

#### 1.2.5 Invalid Names

**Tables**: `person.customer`, `humanresources.employee`

| Invalid Pattern | Error Type | Expected Handling |
|-----------------|--------------|---------------------|
| `"João123"` | Numbers in the name | Validate alphabetic + spaces |
| `"Maria###"` | Special characters | Normalize or reject |
| `"Cliente Teste"` (`"Test Customer"`) | Generic name | Detect test patterns (regex) |
| `"XXXX"` | Placeholder | Flag as invalid |
| `"   "` (spaces only) | Disguised empty string | Trim and validate non-empty |
| `"A"` (1 character) | Name too short | Validate minimum length |

---

### 1.3 Above-Normal Values (1-3%)

**Description**: Legitimate statistical outliers (not fraud), used to test anomaly detection.

**Tables**: `sales.salesorder`, `purchasing.purchaseorderheader`, `warehouse.transaction_history`

| Type | Example | Characteristic | Expected Handling |
|------|---------|----------------|---------------------|
| Luxury order | R$ 150,000 order (average: R$ 300) | Value 500x above average | Flag for manual review, don't auto-block |
| Business purchase | 10,000 pen units (average: 5 units) | Very high quantity | Validate against customer history (B2B vs B2C) |
| Import | 120-day delivery lead time (average: 7 days) | Extreme lead time | Check whether the product is imported |
| International freight | R$ 5,000 freight on a R$ 200 product | Freight > product value | Validate shipping method (express air) |

**Test Scenarios**:
- Outlier detection with Z-score or IQR
- Contextual analysis (B2B vs B2C customer)
- Manual review alerts without automatic blocking
- ML models to separate legitimate outliers from fraud

---

## 2. Fraud and Suspicious Activity

### 2.1 Purchase Fraud (1-2%)

#### 2.1.1 Real Name + Fake CPF

**Table**: `person.customer`, `sales.salesorder`

**Pattern**:
- Apparently legitimate name: `"João da Silva"`
- Obviously fake CPF: `"000.000.000-00"`, `"123.456.789-00"`, `"111.111.111-11"`
- Address may or may not be valid

**Detection**:
- Validate the CPF check digit
- Block known sequences and patterns
- Cross-check against the tax authority database (if available)

#### 2.1.2 Fake Name + Real Address

**Table**: `person.customer`, `sales.salesorder`

**Pattern**:
- Generic/fake name: `"Cliente Teste"` ("Test Customer"), `"Fulano de Tal"` ("John Doe"), `"João Silva"` (very common)
- Real, well-known address (e.g., mall, condominium)
- Delivery to high-traffic locations (harder to trace)

**Detection**:
- Detect very common or generic names (frequency in the base)
- Cross-check the address against commercial-location databases
- Flag deliveries to addresses with multiple different recipients

#### 2.1.3 Cloned Cards (Geographically Impossible Purchases)

**Table**: `sales.salesorder`, `sales.salesorder.payment_info`

**Pattern**:
```json
{
  "order_1": {
    "customer_id": 12345,
    "order_date": "2024-01-15T10:00:00",
    "shipping_city": "São Paulo",
    "shipping_state": "SP"
  },
  "order_2": {
    "customer_id": 12345,
    "order_date": "2024-01-15T10:05:00",  // 5 minutes later
    "shipping_city": "Rio de Janeiro",
    "shipping_state": "RJ"
  }
}
```

**Detection**:
- Calculate geographic distance between consecutive purchases
- Compare time between transactions vs physical travel time
- Flag purchases in different cities on the same day (same customer_id)

#### 2.1.4 Multiple Purchases with Different Cards at the Same Address

**Table**: `sales.salesorder`

**Pattern**:
- Delivery address: `"Rua X, 123, Apto 45"`
- Customer 1: Card ending in `1234`, date: `2024-01-10`
- Customer 2: Card ending in `5678`, date: `2024-01-11`
- Customer 3: Card ending in `9012`, date: `2024-01-12`

**Detection**:
- Group orders by delivery address
- Count unique customer_ids at the same address within a short period (7 days)
- Flag when > 3 different customers deliver to the same location

#### 2.1.5 High Value + Inconsistent Registration Data

**Table**: `sales.salesorder`, `person.customer`

**Pattern**:
- Order of R$ 10,000+
- Free email provider: `joao123@gmail.com`
- Luxury address: `"Av. Brigadeiro Faria Lima, 3000"` (upscale area)
- Recently created account (< 24h)
- No purchase history

**Detection**:
- Cross-check order value against account "age"
- Detect free email providers on high-value purchases
- Check consistency between CEP (upscale area) and purchasing behavior

---

### 2.2 Fraudulent Registrations (1-3%)

#### 2.2.1 Synthetic CPFs (Mathematically Valid, Non-Existent)

**Table**: `person.customer`

**Pattern**:
- CPF passes check-digit validation
- But doesn't exist in the tax authority database
- Generated by public CPF-generation algorithms

**Detection**:
- Integrate with the tax authority API (when available)
- Detect patterns from known CPF-generation tools
- Monitor CPFs with recent creation dates and no history

#### 2.2.2 Multiple Registrations with the Same Phone/Email

**Table**: `person.customer`, `person.customer_email`

**Pattern**:
```json
[
  {"customer_id": 1001, "cpf": "123.456.789-00", "email": "teste@email.com"},
  {"customer_id": 1002, "cpf": "987.654.321-00", "email": "teste@email.com"},
  {"customer_id": 1003, "cpf": "111.222.333-44", "email": "teste@email.com"}
]
```

**Detection**:
- GROUP BY email or phone, count distinct CPFs
- Flag when 1 email has > 2 different CPFs
- Allow exceptions (shared corporate email)

#### 2.2.3 Fake/Non-Existent Addresses

**Table**: `person.address`

**Pattern**:
- `"Rua X, número 99999"` (implausible number)
- Valid CEP but the street doesn't exist for that CEP
- Generic complement: `"Casa"` ("House"), `"Apto 1"`

**Detection**:
- Validate CEP against the postal service API
- Check whether the street matches the CEP
- Flag address numbers > 9999 (implausible)

#### 2.2.4 Impossible Combinations

**Table**: `person.customer`

**Pattern**:
- Brazilian name: `"José da Silva"`
- Unexplained international phone: `"+1 555-1234"` (US)
- Brazilian CPF but address abroad

**Detection**:
- Cross-check phone country against CPF country
- Validate consistency between document nationalities
- Allow exceptions for expatriates (validate against history)

---

### 2.3 Incomplete/Phantom Purchase Processes (2-5%)

#### 2.3.1 Orders Created but Never Paid

**Table**: `sales.salesorder`

**Pattern**:
```json
{
  "salesordernumber": "SO-123456789",
  "order_date": "2024-01-01T10:00:00",
  "order_status": "pending",
  "payment_info": {
    "transactions": [
      {"status": "pending", "method": "credit_card"}
    ]
  },
  "days_since_creation": 45  // > 30 days
}
```

**Detection**:
- Query: `WHERE order_status = 'pending' AND DATEDIFF(NOW(), order_date) > 30`
- Flag for cleanup or automatic cancellation
- Conversion-rate analysis (abandoned cart)

#### 2.3.2 Orders Paid but Never Shipped

**Table**: `sales.salesorder`

**Pattern**:
```json
{
  "salesordernumber": "SO-987654321",
  "order_status": "paid",
  "payment_info": {"transactions": [{"status": "paid"}]},
  "shipping_status": null,  // or "pending" for > 15 days
  "days_since_payment": 20
}
```

**Detection**:
- Query: `WHERE payment_status = 'paid' AND (shipping_status IS NULL OR shipping_status = 'pending') AND days_since_payment > 15`
- Flag operations for investigation
- Indicator of a warehouse or integration problem

#### 2.3.3 Orders Shipped but Never Delivered

**Table**: `sales.salesorder`, `warehouse.transaction_history`

**Pattern**:
```json
{
  "salesordernumber": "SO-555555555",
  "shipping_status": "in_transit",
  "tracking_code": "BR123456789",
  "shipping_date": "2024-01-01",
  "days_in_transit": 120,  // > 90 days
  "last_tracking_update": "2024-01-15"  // stalled for months
}
```

**Detection**:
- Query: `WHERE shipping_status = 'in_transit' AND DATEDIFF(NOW(), shipping_date) > 90`
- Integrate with a tracking API (postal service, carriers)
- Notify the customer and process a refund/reshipment

#### 2.3.4 Orders with Non-Existent Products

**Table**: `sales.salesorder.order_items`

**Pattern**:
```json
{
  "line_number": "SO-123456789-01",
  "product_id": 99999,  // ID that doesn't exist in product.product
  "sku": "SKU-NONEXISTENT",
  "quantity": 1
}
```

**Detection**:
- LEFT JOIN with `product.product` and check for NULL
- Indicator of a system error or a product deleted after the sale
- Flag for data correction or line cancellation

#### 2.3.5 Chargeback Disputes

**Table**: `sales.salesorder.payment_info`

**Pattern**:
```json
{
  "transactions": [
    {
      "status": "paid",
      "dispute_status": "open",
      "dispute_reason": "fraudulent",
      "dispute_date": "2024-02-01"
    }
  ]
}
```

**Detection**:
- Query: `WHERE payment_info->'transactions'->0->>'dispute_status' = 'open'`
- Notify finance and operations
- Block new orders from the customer until resolved

---

### 2.4 Purchasing Fraud (1-2%)

#### 2.4.1 Duplicate Purchase Orders

**Table**: `purchasing.purchaseorderheader`

**Pattern**:
```json
[
  {
    "purchaseorderheader_id": "PO-111111111",
    "vendor_id": 5,
    "order_date": "2024-01-10",
    "total_due": 50000.00
  },
  {
    "purchaseorderheader_id": "PO-222222222",
    "vendor_id": 5,
    "order_date": "2024-01-11",  // 1 day later
    "total_due": 50000.00  // same amount
  }
]
```

**Detection**:
- GROUP BY vendor_id, total_due with date difference < 7 days
- Flag for review (could be an error or a bribe)
- Check whether the purchased items are the same

#### 2.4.2 Phantom Vendors

**Table**: `purchasing.vendor`

**Pattern**:
- Mathematically valid CNPJ
- Company with no economic activity (per tax-authority lookup)
- Generic or non-existent address
- No invoices issued in the last 12 months

**Detection**:
- Integrate with the tax-authority API (registration status)
- Check invoice issuance (SEFAZ)
- Audit vendors with no delivery history

#### 2.4.3 Overpriced Purchases

**Table**: `purchasing.purchaseorderdetail`

**Pattern**:
- Product X: historical average price R$ 100
- New order: price R$ 500 (5x above)
- New vendor or one with no history

**Detection**:
- Compute average and standard deviation per product
- Flag prices > 3 standard deviations above the average
- Require higher-level approval (workflow)

#### 2.4.4 Unauthorized Approvals

**Table**: `purchasing.purchaseorderheader`

**Pattern**:
```json
{
  "purchaseorderheader_id": "PO-123456789",
  "employee_id": 99,  // Employee without approval permission
  "total_due": 100000.00,  // Value above the employee's limit
  "status": "approved"
}
```

**Detection**:
- Cross-check employee_id against the permissions/authority-levels table
- Validate order value vs the employee's approval limit
- Block automatic approvals outside the employee's authority

#### 2.4.5 Deliveries to Personal Addresses

**Table**: `purchasing.purchaseorderheader`, `warehouse.warehouselocation`

**Pattern**:
```json
{
  "purchaseorderheader_id": "PO-987654321",
  "shipping_address": "Rua Residencial, 123, Casa",  // Not a warehouse
  "employee_id": 50  // Employee who approved the order
}
```

**Detection**:
- Validate whether the delivery address is registered in `warehouse.warehouselocation`
- Flag deliveries to addresses not registered as warehouses
- Cross-check the delivery address against the approving employee's address

---

## 3. Warehouse Anomalies (Logistics and Inventory)

### 3.1 Lost/Phantom Transfers (2-5%)

#### 3.1.1 Transfers Never Completed

**Table**: `warehouse.transaction_history`

**Pattern**:
```json
{
  "transaction_id": "TR-123456789",
  "transactiontype_id": 3,  // Transfer
  "traceability_info": {
    "source_warehouselocation_id": 10,
    "target_warehouselocation_id": 20,
    "status": "in_transit"
  },
  "transaction_date": "2024-01-01",
  "days_in_transit": 95  // > 90 days
}
```

**Detection**:
- Query: `WHERE status = 'in_transit' AND DATEDIFF(NOW(), transaction_date) > 90`
- Flag for investigation (loss, theft, system error)
- Adjust inventory if the loss is confirmed

#### 3.1.2 Received Quantity ≠ Sent Quantity

**Table**: `warehouse.transaction_history`

**Pattern**:
```json
{
  "transaction_send": {
    "transaction_id": "TR-111111111",
    "quantity": 100,
    "source_warehouselocation_id": 10
  },
  "transaction_receive": {
    "transaction_id": "TR-111111111-RCV",
    "quantity": 95,  // 5 units missing
    "target_warehouselocation_id": 20,
    "traceability_info": {
      "quality_check_status": "pass",
      "notes": "5 units damaged in transit"
    }
  }
}
```

**Detection**:
- Cross-check outbound transactions with inbound ones (same reference_order_id)
- Compute divergence: `(received - sent) / sent * 100`
- Flag if the divergence > 5%

#### 3.1.3 Transfers to the Same Location

**Table**: `warehouse.transaction_history`

**Pattern**:
```json
{
  "transaction_id": "TR-999999999",
  "transactiontype_id": 3,
  "traceability_info": {
    "source_warehouselocation_id": 10,
    "target_warehouselocation_id": 10  // SAME location!
  }
}
```

**Detection**:
- Query: `WHERE source_warehouselocation_id = target_warehouselocation_id`
- Indicator of a system error or misconfiguration
- Block transfers where source = destination at the API level

#### 3.1.4 Transactions Without Traceability

**Table**: `warehouse.transaction_history`

**Pattern**:
```json
{
  "transaction_id": "TR-888888888",
  "quantity": 50,
  "traceability_info": null  // No traceability!
}
```

**Detection**:
- Query: `WHERE traceability_info IS NULL`
- Require traceability_info on critical transactions
- Flag for audit (possible fraud or error)

---

### 3.2 Inventory Discrepancies (3-8%)

#### 3.2.1 Physical Quantity ≠ System Quantity

**Table**: `warehouse.product_inventory`

**Pattern**:
```json
{
  "product_id": 123,
  "warehouselocation_id": 10,
  "quantity_on_hand": 100,  // System
  "physical_count": 95,     // Physical count
  "last_cycle_count": "2024-01-15",
  "variance": -5
}
```

**Detection**:
- Implement cycle counts
- Compute: `variance = physical_count - quantity_on_hand`
- Flag if |variance| > 5% or absolute value > 10 units

#### 3.2.2 Products with Negative or Zero Cost

**Table**: `warehouse.product_inventory`, `warehouse.transaction_history`

**Pattern**:
```json
{
  "product_id": 456,
  "actual_cost": -10.50  // Negative!
}
```

**Detection**:
- Query: `WHERE actual_cost <= 0`
- Indicator of a system error, botched reversal, or bug
- Block negative costs at the API level (Pydantic validation)

#### 3.2.3 Products Available Without a Physical Location

**Table**: `warehouse.product_inventory`

**Pattern**:
```json
{
  "product_id": 789,
  "quantity_on_hand": 50,
  "warehouselocation_id": null  // No location!
}
```

**Detection**:
- Query: `WHERE quantity_on_hand > 0 AND warehouselocation_id IS NULL`
- Blocks picking (operator doesn't know where to look)
- Require a physical location before marking as available

#### 3.2.4 Negative Stock (Overselling)

**Table**: `warehouse.product_inventory`

**Pattern**:
```json
{
  "product_id": 101,
  "quantity_on_hand": -10  // Negative!
}
```

**Detection**:
- Query: `WHERE quantity_on_hand < 0`
- Indicator of overselling (sold more than was in stock)
- Implement validation at time of sale (ATP - Available to Promise)

#### 3.2.5 Movements Without a Source Order

**Table**: `warehouse.transaction_history`

**Pattern**:
```json
{
  "transaction_id": "TR-777777777",
  "transactiontype_id": 2,  // Outbound for sale
  "quantity": 10,
  "reference_order_id": null  // No linked sales order!
}
```

**Detection**:
- Query: `WHERE transactiontype_id = 2 AND reference_order_id IS NULL`
- Indicator of an unauthorized movement (possible theft)
- Require a source order for outbound transactions

---

### 3.3 Quality Problems (1-2%)

#### 3.3.1 Products Failed QC but Shipped Anyway

**Table**: `warehouse.transaction_history`, `sales.salesorder`

**Pattern**:
```json
{
  "transaction_id": "TR-666666666",
  "product_id": 202,
  "traceability_info": {
    "quality_check_status": "fail"  // Failed!
  },
  "reference_order_id": 12345  // But it was sold!
}
```

**Detection**:
- Query: `WHERE quality_check_status = 'fail' AND reference_order_id IS NOT NULL`
- Indicator of a process error or fraud
- Block shipment of products with status != "pass"

#### 3.3.2 Cold Chain Break (IoT)

**Table**: `warehouse.transaction_history.traceability_info.iot_sensor_data`

**Pattern**:
```json
{
  "product_id": 303,
  "product_category": "Refrigerated Foods",
  "temp_at_scan": 15.5,  // OK temperature: 2-8°C expected
  "expected_range": {"min": 2.0, "max": 8.0},
  "iot_sensor_data": {
    "temp_at_scan": 15.5,  // OUT OF RANGE!
    "device_id": "IOT-SCANNER-123"
  }
}
```

**Detection**:
- Define temperature ranges per product category
- Query: `WHERE temp_at_scan NOT BETWEEN min_temp AND max_temp`
- Flag and block shipment of products with out-of-range temperature

#### 3.3.3 Expired Products in Active Stock

**Table**: `warehouse.product_inventory`, `product.product`

**Pattern**:
```json
{
  "product_id": 404,
  "expiration_date": "2023-12-31",  // Expired!
  "quantity_on_hand": 20,
  "status": "available"  // Still available for sale!
}
```

**Detection**:
- Query: `WHERE expiration_date < NOW() AND status = 'available'`
- Implement a FEFO (First Expired, First Out) routine
- Flag for disposal or sales block

---

### 3.4 Negative Values (2-4%)

**Table**: `warehouse.transaction_history`, `sales.salesorder`, `purchasing.purchaseorderheader`

| Field | Invalid Pattern | Error Type | Expected Handling |
|-------|-----------------|--------------|---------------------|
| `actual_cost` | `-50.00` | Botched reversal | Investigate the reversal transaction, adjust the cost |
| `quantity` | `-10` | Integration bug | Block negative quantities at the API level |
| `freight` | `-100.00` | Poorly implemented system adjustment | Validate the freight-discount logic |
| `total_due` | `-500.00` | Calculation error | Recompute the total, validate business rules |
| `tax_amt` | `-18.00` | Negative tax (impossible) | Block in Pydantic validation |

**Detection**:
- Query: `WHERE actual_cost < 0 OR quantity < 0 OR freight < 0 OR total_due < 0`
- Implement `ge=0` (greater than or equal) validation in Pydantic schemas
- Allow negative values only in adjustment/reversal tables (with an audit trail)

---

## 3.5 Human Resources Anomalies (1-3%)

**Important context**: unlike `sales`/`purchasing`/`warehouse`, the `humanresources.employee` seeder (`dbapi/api/seeder/humanresources/humanresources_employee.py`) currently **does not generate real history** — every record is born with `is_current=True` and is never closed by a `MERGE`/upsert that compares it to the previous version. This means the Silver layer **cannot** fix these anomalies by looking at "this same employee's previous value" (it doesn't exist). The correction strategies below instead use: (a) reference/dimension data (e.g. `humanresources.jobrole`, which has its own SCD2 with `min_salary`/`max_salary`), (b) peer statistics (median for the same role/department), or (c) deterministic business rules — never "the individual's last known value".

### 3.5.1 Salary Outside the Role's Range

**Tables**: `humanresources.employee` (`current_salary`) + `humanresources.jobrole` (`min_salary`, `max_salary`, `is_current=true`)

**Pattern**:
```json
{
  "employee_id": 42,
  "job_role_id": 7,
  "current_salary": 55000.00,
  "jobrole_range": {"min_salary": 8000.00, "max_salary": 22000.00}
}
```

**Detection**:
```sql
SELECT e.employee_id, e.current_salary, j.min_salary, j.max_salary
FROM humanresources.employee e
JOIN humanresources.jobrole j
  ON e.job_role_id = j.jobrole_id AND j.is_current = true
WHERE e.current_salary NOT BETWEEN j.min_salary AND j.max_salary;
```

**Silver correction (without the individual's history)**:
- **Quarantine, not silent overwrite**: add a derived `salary_flag` column (`'above_range'` / `'below_range'` / `'ok'`). Salary is sensitive data — the definitive correction is an HR decision, not the pipeline's.
- If Gold needs a "clean" value for aggregate metrics, generate `current_salary_corrected = LEAST(GREATEST(current_salary, min_salary), max_salary)` **in a separate column**, preserving the original `current_salary` for audit purposes.
- Statistical alternative: impute using the **peer salary median** (`PERCENTILE_CONT(0.5) ... PARTITION BY job_role_id` over records already within range), instead of a fixed value.

### 3.5.2 Salesperson Without a Commission

**Table**: `humanresources.employee` (`role_attributes` JSONB, when the role is Sales)

**Note on the schema**: `role_attributes: Union[SalesRoleAttributes, ..., Dict[str, Any]]` has a fallback to `Dict[str, Any]`. That's what lets the chaos payload (missing `comission_pct`) into Bronze without breaking Pydantic validation — the fallback should intentionally stay in place for this scenario.

**Pattern**: `role_attributes->>'comission_pct' IS NULL OR = 0` with `job_title LIKE '%Vendas%'` / `%Sales%`.

**Silver correction**: same principle already used in the legacy AdventureWorks pipeline (`spark/jobs/silver_layer_advworks_scripts/advdim_salesperson.py::calculateSalesPersonCommission`) — backfill from a **default commission table/dict by territory or role tier** (e.g. `{"North America": 0.05, "Europe": 0.04, "Pacific": 0.04}`), never from the employee's previous value.

### 3.5.3 Salesperson Without a Territory/Region

**Table**: `humanresources.employee` (`role_attributes.territory_codes`)

**Difference from AdventureWorks**: there, a dedicated territory dimension exists (`territoryid`); in dbapi there isn't one — the salesperson only carries `country_id`/`state_province_id`.

**Silver correction**: deterministic backfill using the geography already present on the record itself (`country_id` + `state_province_id`), with a static country/state → territory mapping (same pattern as `transformSalesPersonTerritory`, but keyed by geography instead of role string).

### 3.5.4 Inconsistent Hierarchy (Manager in a Different Department)

**Table**: `humanresources.employee` (`manager_id`, `department_id`)

**Pattern**: `manager_id` points to an employee in a `department_id` different from the subordinate's.

**Detection**: self-join on `employee` comparing the employee's `department_id` with their `manager_id`'s.

**Correction**: only flag it (`hierarchy_flag`) for manual review — a matrix reporting structure can be legitimate and shouldn't be "corrected" automatically.

### 3.5.5 Broken SCD2 — Multiple "Current" Records or Temporal Gaps

**Table**: `humanresources.employee`

**Pattern**: more than one record with `is_current=true` for the same `employee_id`, or a version's `effective_end_date` that doesn't match the next version's `effective_start_date`.

**Detection**:
```sql
-- More than one "current" version
SELECT employee_id, COUNT(*)
FROM humanresources.employee
WHERE is_current = true
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

**Correction**: this is the only rule in the section that **builds history instead of relying on it** — in Silver, use `ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_start_date DESC)` to keep only the most recent version as `is_current=true` and close the rest (`effective_end_date = next effective_start_date`). Structural recommendation: move this "close the previous version" logic into the **load process** (upsert/MERGE by `employee_id` comparing tracked attributes), not just cleanup — that's what fixes the root cause of "no history exists" mentioned above.

### 3.5.6 Terminated Employee Still Marked as Active

**Table**: `humanresources.employee`

**Pattern**: `end_date` is set (employee left) but `is_active=true` and/or `is_current=true`.

**Detection**: `WHERE end_date IS NOT NULL AND (is_active = true OR is_current = true)`

**Correction**: deterministic rule (doesn't depend on history) — Silver closes the record automatically: `is_active=false`, `is_current=false`, `effective_end_date=end_date`.

---

## 3.6 Marketing Anomalies (1-5%)

**Important context — the Pydantic Guard**: unlike `sales`/`purchasing`/`warehouse`/`humanresources` (where the seeders insert a raw `dict` via SQLAlchemy), the 4 `marketing` seeders (`marketing_campaign.py`, `marketing_adgroup.py`, `marketing_ads.py`, `marketing_daily_performance.py`) **validate the payload through the Pydantic schema before insert** (`Model(**payload).model_dump()`). This means **not every chaos type passes straight through** — each rule below states whether it needs to bypass validation, or whether it already passes normally because the current schema doesn't cover that scenario.

| Rule | Needs Pydantic bypass? | Reason |
|---|---|---|
| 3.6.1 Broken naming convention | No | `campaign_name`/`utm_campaign` are free-form `str`, no regex |
| 3.6.2 Zeroed budget + active status | No | `total_budget: Decimal = Field(ge=0.0)` accepts `0.0` |
| 3.6.3 Budget overrun (300%) | No* | not validated at the campaign schema level; it's a cross-table check (campaign × sum of `daily_performance.cost`) |
| 3.6.4 Empty targeting `{}` | **Yes** | `device_types: List[DeviceType]` is required, no default |
| 3.6.5 Inverted age range (min > max) | **Yes** | blocked by a `model_validator` on `AgeRange`, not by `Field` — "loosening" fields doesn't fix it, you need to skip the validator or the whole schema |
| 3.6.6 Broken links | **Yes** | `final_url: HttpUrl` and `MediaAsset.url: AnyUrl` are strict |
| 3.6.7 Media type incompatible with extension | No | there's no cross-validation of `media_type` × extension today |
| 3.6.8 Field leakage between platforms | No | `MarketingMetrics` accepts any combination of optional fields |
| 3.6.9 Clicks > Impressions | No | only an individual `ge=0` exists on each field |
| 3.6.10 Zero cost with conversion | No | same as above |
| 3.6.11 Retroactive metrics (revision) | No** | not a schema problem; it's the same `(ad_id, date_ref)` inserted twice with different values — requires the loader to allow reprocessing |
| 3.6.12 Non-monotonic video funnel | No | no order check between `video_views_25/50/75/100` |
| 3.6.13 Reach > Impressions | No | same, no cross-check |
| 3.6.14 Duplicate UTM across campaigns | No | `utm_campaign` has no uniqueness constraint |

\* requires performance already seeded for the campaign (execution order: campaign → adgroup → ads → daily_performance).
\*\* requires the load process to allow inserting a new row for an already-existing `(ad_id, date_ref)` key (today the seeder only does `insert()`, with no `ON CONFLICT`/upsert and no unique constraint — running the seeder twice would already duplicate days unintentionally).

### 3.6.1 Broken Naming Convention

**Table**: `marketing.campaign` (`campaign_name`, `utm_campaign`)

**Pattern**: 5% of campaigns deviate from the `{PLATFORM}_{OBJECTIVE}_{VARIANT}` pattern (e.g. `GSN_Brand_Retargeting`) and come in as free text typed by a human operator: `"Cópia de campanha teste 1"` ("Copy of test campaign 1"), `"nova campanha (1)"` ("new campaign (1)").

**Detection**: `WHERE campaign_name !~ '^[A-Z]+_[A-Za-z]+_[A-Za-z]+$'` — tests whether the regex-based UTM extraction scripts survive an off-pattern name.

**Silver correction**: don't try to "guess" the UTM from the free-form name — extract from `utm_campaign` (structured field) when available; if that's also missing/malformed, flag `attribution_flag = 'unparsable_name'` instead of forcing a generic regex.

### 3.6.2 Zeroed Budget with Active Status

**Table**: `marketing.campaign`

**Pattern**: `total_budget = 0.00` with `status = 'active'`.

**Detection**: `WHERE total_budget = 0 AND status = 'active'`

**Correction**: flag it (`budget_flag = 'zero_budget_active'`) for review — it may actually be a paused campaign with a stale status in the API (common when the integration doesn't sync status in real time).

### 3.6.3 Budget Overrun (Overspend)

**Table**: `marketing.campaign` + `marketing.daily_performance` (sum of `cost`)

**Pattern**: for ~5% of campaigns, the sum of `cost` in `daily_performance` exceeds `total_budget` by up to 300%.

**Detection**:
```sql
SELECT c.campaign_id, c.total_budget, SUM(p.cost) as total_spent,
       (SUM(p.cost) / NULLIF(c.total_budget, 0)) as spend_ratio
FROM marketing.campaign c
JOIN marketing.daily_performance p ON p.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.total_budget
HAVING SUM(p.cost) > c.total_budget * 1.5;
```

**Correction**: never truncate the real `cost` — it's financial data from the ad platform. Just flag it (`overspend_flag`) and, if Gold needs it, expose `budget_utilization_pct` as a derived metric.

### 3.6.4 Missing/Empty Targeting

**Table**: `marketing.adgroup` (`targeting_criteria`)

**Pattern**: `targeting_criteria = {}` in 10% of AdGroups, as if the integration lacked permission to read audience data for that account.

**Detection**: `WHERE targeting_criteria = '{}'::jsonb OR targeting_criteria->'device_types' IS NULL`

**Correction**: don't invent a targeting criterion — fill in only the minimum that can be inferred (e.g. `device_types` defaulting to `["desktop", "mobile"]` as a documented fallback) and flag `targeting_flag = 'missing_audience_data'` to exclude it from segmentation analysis until it's fixed at the source.

### 3.6.5 Inverted Age Range

**Table**: `marketing.adgroup` (`targeting_criteria.age_range`)

**Pattern**: `{"min_age": 65, "max_age": 18}`.

**Detection**: `WHERE (targeting_criteria->'age_range'->>'min_age')::int > (targeting_criteria->'age_range'->>'max_age')::int`

**Correction**: deterministic `swap` (`LEAST`/`GREATEST` on the two values) — unlike salary (HR), there's no business ambiguity here: a swapped min/max age is always a typo/submission error, safe to auto-correct.

### 3.6.6 Broken Links (Creatives)

**Table**: `marketing.ads` (`creative_data.final_url`, `creative_data.media_assets[].url`)

**Pattern**: URL without `https://` (`"www.example.com/lp"`) or with spaces (`"https://cdn.example.com/ad final.jpg"`).

**Detection**: `WHERE final_url !~ '^https?://' OR final_url ~ '\s'`

**Correction**: normalize (`trim` + prefix `https://` when plausible) and, if still invalid after normalization, flag `creative_flag = 'broken_url'` — never let a broken link flow into per-creative CTR reports without a flag.

### 3.6.7 Media Type Incompatible with Extension

**Table**: `marketing.ads` (`creative_data.media_type` vs `creative_data.media_assets[].url`)

**Pattern**: `media_type = 'video'` but the only URL in `media_assets` ends in `.jpg`/`.png`.

**Detection**: `WHERE media_type = 'video' AND media_assets[0]->>'url' ~ '\.(jpg|jpeg|png)$'`

**Correction**: reclassify `media_type` based on the actual file extension (the asset is the source of truth, not the API metadata) and flag `creative_flag = 'media_type_mismatch'`.

### 3.6.8 Field Leakage Between Platforms

**Table**: `marketing.daily_performance` (`metrics`, `quality_score` field, exclusive to Google Ads)

**Pattern**: a `meta_ads`/`tiktok_ads` channel ad has `metrics.quality_score` populated.

**Detection**:
```sql
SELECT p.performance_id
FROM marketing.daily_performance p
JOIN marketing.ads a ON a.ad_id = p.ad_id
JOIN marketing.adgroup ag ON ag.ad_group_id = a.ad_group_id
JOIN marketing.campaign c ON c.campaign_id = ag.campaign_id
JOIN marketing.channel ch ON ch.channel_id = c.channel_id
WHERE ch.platform_name != 'google_ads'
  AND p.metrics->>'quality_score' IS NOT NULL;
```

**Correction**: null out the field outside the platform's domain (`quality_score = NULL` when `platform_name != 'google_ads'`) before writing to the Silver Wide Table — per-platform field mapping must be explicit, not inherited from the raw payload.

### 3.6.9 Clicks > Impressions

**Table**: `marketing.daily_performance`

**Pattern**: `clicks > impressions` (common due to bots or platform reporting delays).

**Detection**: `WHERE clicks > impressions`

**Correction**: don't zero out or discard — clamp `impressions` to `GREATEST(impressions, clicks)` in a derived column and flag `metric_flag = 'clicks_exceed_impressions'` for audit, keeping the original values intact.

### 3.6.10 Zero Cost with Conversion

**Table**: `marketing.daily_performance`

**Pattern**: `cost = 0.00` and `conversions > 0` (e.g. a sale attributed days later, within the attribution window, with no cost recorded on that day).

**Detection**: `WHERE cost = 0 AND conversions > 0`

**Correction**: not an error to fix — it's expected deferred-attribution behavior. Just flag `attribution_flag = 'delayed_conversion'` so it doesn't distort daily ROAS/CPA metrics (the real cost was already recorded on a different `date_ref`).

### 3.6.11 Metrics That Change Retroactively

**Table**: `marketing.daily_performance`

**Pattern**: the same `(ad_id, date_ref)` key receives a second load, days later, with different `conversions`/`conversion_value` (Google/Meta attribution-window revision).

**Detection**:
```sql
SELECT ad_id, date_ref, COUNT(*) as versions,
       COUNT(DISTINCT conversions) as distinct_conversion_values
FROM marketing.daily_performance
GROUP BY ad_id, date_ref
HAVING COUNT(*) > 1;
```

**Correction**: requires a change in the load strategy, not just cleanup — implement upsert (`MERGE`/`ON CONFLICT (ad_id, date_ref) DO UPDATE`) keeping the most recent version by `updated_at`, and optionally an audit table (`daily_performance_revisions`) to track what changed. Today the seeder has no unique constraint on `(ad_id, date_ref)`, so this is the same structural gap as [3.5.5](#355-broken-scd2--multiple-current-records-or-temporal-gaps) (broken SCD2), now in the fact domain instead of a dimension.

### 3.6.12 Non-Monotonic Video Funnel

**Table**: `marketing.daily_performance` (`metrics.video_views_25/50/75/100`)

**Pattern**: `video_views_75 > video_views_50` (physically impossible — anyone who reached 75% also passed through 50%).

**Detection**: `WHERE (metrics->>'video_views_100')::int > (metrics->>'video_views_75')::int OR (metrics->>'video_views_75')::int > (metrics->>'video_views_50')::int OR (metrics->>'video_views_50')::int > (metrics->>'video_views_25')::int`

**Correction**: clamp in cascade (`video_views_75 = LEAST(video_views_75, video_views_50)`, etc.) in derived columns, preserving the raw values.

### 3.6.13 Reach > Impressions

**Table**: `marketing.daily_performance` (`metrics.reach`)

**Pattern**: `reach` (unique reach) greater than `impressions` (total, including repeats) — physically impossible.

**Detection**: `WHERE (metrics->>'reach')::int > impressions`

**Correction**: same pattern as 3.6.9/3.6.12 — clamp in a derived column + flag, never overwrite the raw data.

### 3.6.14 Duplicate UTM Across Campaigns

**Table**: `marketing.campaign` (`utm_campaign`)

**Pattern**: two different `campaign_id`s sharing the same `utm_campaign` (common human error: an analyst reuses the string from an old campaign).

**Detection**:
```sql
SELECT utm_campaign, COUNT(DISTINCT campaign_id) as campaigns
FROM marketing.campaign
GROUP BY utm_campaign
HAVING COUNT(DISTINCT campaign_id) > 1;
```

**Correction**: not automatically fixable (there's no way to know which campaign is the "right" one for that UTM) — flag `attribution_flag = 'duplicate_utm'` and exclude it from UTM-based attribution in Gold until manually resolved, using `campaign_id` (the technical key) as a grouping fallback.

---

## 3.7 Person Anomalies — Customer, Email, and Geography (1-5%)

**Bug fixed in this section**: `person_customer.py` drew `country_id` and `state_province_id` from two **global, independent** lists (all countries × all states), with no join between them — meaning **100% of customers** could be born with a state that doesn't belong to the drawn country, not as a controlled exception. The seeder now builds a `states_by_country` map (via `StateProvinceModel.country_id`) and only draws countries that already have a registered state, guaranteeing the state always belongs to the country by construction. As a result, the geographic inconsistency (3.7.6) goes back to being a rare, deliberate exception, not the default behavior.

### 3.7.1 Name Formatting (Case and Spaces)

**Table**: `person.customer` (`customer_name`, `last_name`)

**Pattern**: 5% of names come in ALL CAPS (`"JOÃO DA SILVA"`), 5% in lowercase (`"joão da silva"`), or with extra spaces at the ends (`"  João da Silva  "`) — applied to the full name (`customer_name` + `last_name` together), not to each field individually.

**Detection**: `WHERE customer_name != TRIM(customer_name) OR customer_name = UPPER(customer_name) OR customer_name = LOWER(customer_name)`

**Silver correction**: `TRIM()` + `INITCAP()` to normalize, preserving the original value in an audit column if the Data Quality pipeline needs to track the dirty-data rate over time.

### 3.7.2 Illogical Birth Dates

**Table**: `person.customer` (`birthdate`)

**Pattern**: born in the future (years 2030-2050) or an impossible age (120-150 years) — simulating a date-entry typo.

**Detection**: `WHERE birthdate > NOW() OR birthdate < NOW() - INTERVAL '110 years'`

**Correction**: not automatically fixable (there's no way to know the real date) — flag `birthdate_flag = 'invalid'` and exclude it from age/demographic metrics until manually corrected or re-collected.

### 3.7.3 Formatted vs Unformatted CPF/CNPJ

**Table**: `person.customer` (`social_number`)

**Pattern**: already implemented in `api/samples/customer.py` (50% with punctuation `123.456.789-00`, 50% digits only `12345678900`).

**Correction**: normalize via regex (strip everything that isn't a digit) before any JOIN or comparison across tables that use `social_number`.

### 3.7.4 Critical Email Syntax

**Table**: `person.customer_email` (`customer_email`)

**Pattern**: comma instead of a period (`doug@gmail,com`), missing TLD (`doug@gmail`) — in addition to what already existed (email without `@`, space before the `@`).

**Detection**: `WHERE customer_email !~ '^[^@]+@[^@]+\.[a-zA-Z]{2,}$'`

**Correction**: normalization regex + RFC 5322 validation; if not recoverable, flag `email_flag = 'invalid'` and exclude it from marketing campaigns (don't generate a bounce at a real provider).

### 3.7.5 Multiple Values Hidden in the Same Field

**Table**: `person.customer_email` (`customer_email`)

**Pattern**: two emails in the same record, separated by `; ` (e.g. `"doug@gmail.com; doug.work@empresa.com"`), instead of two rows — a classic error from a form with no single-field validation.

**Detection**: `WHERE customer_email ~ '[;,]\s*\S+@\S+'`

**Correction**: split on `;`/`,` and materialize as additional records (`is_primary=false`) in the same table, since the correct modeling is 1 row per email.

### 3.7.6 "Fake Foreigner" (Inconsistent Geography)

**Table**: `person.customer` (`country_id`, `state_province_id`)

**Pattern**: `state_province_id` points to a state that belongs to a different country than the record's `country_id` (e.g. state "São Paulo" with country "United States").

**Detection**:
```sql
SELECT c.customer_id
FROM person.customer c
JOIN person.stateprovince sp ON sp.state_province_id = c.state_province_id
WHERE sp.country_id != c.country_id;
```

**Correction**: there's no way to "guess" which of the two fields is right — flag `geography_flag = 'country_state_mismatch'` for manual review. Governance note: since there's no *check constraint* in the database tying `state_province_id` to `country_id` (JSONB and loose keys don't natively support this in `person.customer`), this inconsistency **must** be caught in Silver — Postgres won't prevent it on its own.

### 3.7.7 Lowercase Geographic Code

**Tables**: `person.countryregion` (`country_code`), `person.stateprovince` (`state_province_code`)

**Pattern**: 5% of codes come in lowercase (`br` instead of `BR`), simulating a third-party API integration that doesn't normalize before sending.

**Detection**: `WHERE country_code != UPPER(country_code)`

**Correction**: normalize with `UPPER()` — since these are small, stable dimension tables, the ideal fix is at the source (a `CHECK (country_code = UPPER(country_code))` constraint) in addition to normalizing in Silver.

---

## 3.8 Product Anomalies — Catalog, Category, and Embeddings (1-5%)

**New columns added in this section** (didn't exist before — without them, the scenarios below wouldn't even be representable):
- `product.product.sell_start_date` / `sell_end_date` (`sell_end_date IS NULL` = product still on sale)
- `product.productcategory.parent_category_id` (self-FK, `NULL` = root category)

**Identified gaps that remain out of scope** (documented here so they aren't mistaken for bugs):
- `product.productmodel` isn't a free-form "product model" table — it's a fixed enum of ~19 types (`SMARTPHONE`, `LAPTOP_COMPUTING`, etc.), with no rich-text description field. The "leaking HTML tags" and "near-duplicate names" anomalies don't apply to this table; the duplicate-name one was implemented on `product.product_name` (3.8.3), which is genuinely free text.
- `product.productcategory` has no real hierarchy across the 19 categories (they're all siblings) — `parent_category_id` was added as a column, but the seeder keeps every category as root (`NULL`) by default; only the chaos path forces a value.

### 3.8.1 Negative Margin / Zeroed Price

**Table**: `product.product` (`productdetails.price`, `productdetails.list_price`)

**Pattern**: `price` (sale) ends up below `list_price` (production cost), or zeroed — simulating a misconfigured promotion glitch. Note the field names are counterintuitive: `list_price` = cost, `price` = sale price.

**Detection**: `WHERE (productdetails->>'price')::numeric < (productdetails->>'list_price')::numeric OR (productdetails->>'price')::numeric = 0`

**Correction**: never auto-correct (sensitive financial data) — flag `margin_flag = 'negative_or_zero'` and alert for manual review before any profit report consumes the row.

### 3.8.2 Weight with a Dirty Unit (String Instead of Numeric)

**Table**: `product.product` (`productdetails.weight`)

**Pattern**: instead of `1.5` (clean float), the field comes as `"1.5kg"`, `"1,5 kg"`, or `"1.5 KG"` — typical of integration with a legacy system that concatenates the unit into the value.

**Detection**: `WHERE productdetails->>'weight' !~ '^[0-9]+(\.[0-9]+)?$'`

**Correction**: regex to extract the numeric part + normalize the decimal separator (`,` → `.`) before `CAST`ing to `Numeric` in Silver.

### 3.8.3 Near-Duplicate Product Name

**Table**: `product.product` (`product_name`)

**Pattern**: two products with the same base name but different formatting (`"Smartphone Apple - Preto"` vs `"Smartphone-Apple---Preto"`) — a classic double-registration error.

**Detection**: `GROUP BY LOWER(REPLACE(product_name, '-', ' ')) HAVING COUNT(DISTINCT product_id) > 1`

**Correction**: normalize (`lower` + collapse separators) before any deduplication; don't auto-delete — both `product_id`s may already have associated sales.

### 3.8.4 Illogical Sale Dates

**Table**: `product.product` (`sell_start_date`, `sell_end_date`)

**Pattern**: `sell_end_date` before `sell_start_date`.

**Detection**: `WHERE sell_end_date IS NOT NULL AND sell_end_date < sell_start_date`

**Correction**: flag `lifecycle_flag = 'invalid_sell_window'` — it's not safe to automatically infer which date is correct.

### 3.8.5 Orphan Category (Non-Existent Parent)

**Table**: `product.productcategory` (`parent_category_id`)

**Pattern**: `parent_category_id` points to a `category_id` that never existed (simulating a deleted parent category with no cascade).

**Detection**:
```sql
SELECT c.category_id
FROM product.productcategory c
LEFT JOIN product.productcategory p ON p.category_id = c.parent_category_id
WHERE c.parent_category_id IS NOT NULL AND p.category_id IS NULL;
```

**Correction**: recursive CTEs in the Data Warehouse should treat an orphan `parent_category_id` as root (`COALESCE` in the join, don't fail the query), in addition to alerting the catalog team.

### 3.8.6 Cyclic Category

**Table**: `product.productcategory` (`parent_category_id`)

**Pattern**: two categories pointing at each other (`A.parent_category_id = B` and `B.parent_category_id = A`) — tests whether the Data Warehouse's recursive CTE has infinite-loop protection.

**Detection**: a recursive CTE with no `LIMIT`/max depth will hang on this case — the detection query itself needs a depth guard (`WHERE depth < 20`) so it doesn't hang too.

**Correction**: add `MAX RECURSION DEPTH` (or equivalent) to every recursive CTE over `productcategory`, and alert when the limit is reached (indicates a cycle, not a legitimate hierarchy).

### 3.8.7 Truncated Embedding Vector (Dimension Mismatch)

**Table**: `product.productembedding` (`description_vector`)

**Original pattern (revised)**: the first version of the seeder tried to insert a 1535-dimension vector instead of 1536, simulating a silent failure in the inference step. In practice this is **never silent** — the column is `vector(1536)` (fixed dimension for pgvector), the `INSERT` fails immediately with a dimension error, and since the seeder does a single `insert_in_batches` per run with no intermediate commit or per-batch handling, this failure takes down the whole transaction: **zero embeddings** get written in that run, not just the corrupted record (confirmed by running `nexomat_seeder_dag` end to end — every time this chaos variant was drawn, the entire seeder exited without inserting anything).

**Current implementation**: instead of generating a wrong-sized array, `description_vector` is left `NULL` and `ai_metadata.text_model = "truncated_vector_error"` — the same pattern already used for `failed_image_extraction`. This represents the equivalent real-world scenario (the embedding pipeline discarded the malformed result instead of letting it leak into the database) without breaking the whole batch's INSERT.

**Detection**: `SELECT product_id FROM product.productembedding WHERE description_vector IS NULL AND ai_metadata->>'text_model' = 'truncated_vector_error'`

### 3.8.8 Extraction Failure with NaN/Infinite Values

**Table**: `product.productembedding` (`description_vector`)

**Original pattern (revised)**: the first version wrote `NaN`/`Infinity`/`-Infinity` into 1 to 5 vector positions. The assumption that "pgvector silently accepts it" was **wrong** — tested empirically, the INSERT fails with `DataError: infinite value not allowed in vector` (pgvector validates and rejects NaN/Infinity on write), with the same side effect of zeroing out the whole run described in 3.8.7.

**Current implementation**: same handling — `description_vector = NULL` and `ai_metadata.text_model = "nan_infinite_extraction_error"`.

**Detection**: `SELECT product_id FROM product.productembedding WHERE description_vector IS NULL AND ai_metadata->>'text_model' = 'nan_infinite_extraction_error'`

**Correction (both cases)**: a null vector means the embedding job failed and the product needs reprocessing — there's no corrupted vector to "fix", just re-queue for reprocessing.

---

## 3.9 Purchasing Anomalies (1-5%)

**Bugs fixed in this section** (found while validating the rules, not just missing chaos):
- The 4 `purchasing` seeders had broken imports (`api.models.purchasing.orderheader`/`orderdetail` don't exist — the real modules are `purchaseorderheader.py`/`purchaseorderdetail.py`; `api.schemas.purchase_audit` doesn't exist — the real one is `api.schemas.purchasing.purchasing_orderaudit`; `product_id` was being looked up in `ProductModel` (`productmodel.py`, a type table), not `Product` (`product.py`, the real product table)). None of the 4 ran before this fix.
- `purchase_purchaseorderdetail.py` compared `h["status"] == 4` to decide whether an order was received — but `status` is stored as a string (`"completed"`, via `Status.value`), never `4`. Result: `received_qty`/`stocked_qty`/`is_fully_received` always ended up `0`/`0`/`False`, regardless of the real status. Fixed to `h["status"] == Status.COMPLETED.value`.
- `purchase_purchaseorderaudit.py` had no load function (`async def seed_...`) and no `__main__` — there was no entry point to run it via Airflow/CLI, unlike the other 3 seeders in the domain. Added.

### 3.9.1 Financial Math That Doesn't Add Up

**Table**: `purchasing.purchaseorderheader`

**Pattern**: a "manual discount" is applied directly to `total_due`, without recalculating `sub_total`/`tax_amt`/`freight` — `sub_total + tax_amt + freight != total_due`.

**Detection**: `WHERE total_due != (sub_total + tax_amt + freight)`

**Correction**: never auto-overwrite `total_due` (it's financial data someone approved) — flag `accounting_flag = 'total_mismatch'` and alert finance for manual review of the discrepancy.

### 3.9.2 Ship Date Before Order Date

**Table**: `purchasing.purchaseorderheader`

**Pattern**: `ship_date` earlier than `order_date` — logistics time travel.

**Detection**: `WHERE ship_date < order_date`

**Correction**: flag `logistics_flag = 'invalid_ship_window'`; it's not safe to infer which date is correct.

### 3.9.3 Phantom Order

**Table**: `purchasing.purchaseorderheader`

**Pattern**: `status = 'completed'` with every financial value (`sub_total`, `tax_amt`, `freight`, `total_due`) zeroed.

**Detection**: `WHERE status = 'completed' AND total_due = 0.00`

**Correction**: block invoicing of zero-value orders via a business rule in the service layer (not just detect it after the fact) — a "completed" order with no value at all is, by definition, a process failure.

### 3.9.4 Over-Shipment (Physically Impossible)

**Table**: `purchasing.purchaseorderdetail`

**Pattern**: `received_qty + rejected_qty > order_qty` — the vendor shipped more than was ordered and the warehouse logged it in without validating.

**Detection**: `WHERE received_qty + rejected_qty > order_qty`

**Correction**: flag `receiving_flag = 'overshipment'` for the receiving team; don't auto-truncate `received_qty` (could be fraud or a counting error, needs investigation).

### 3.9.5 Zombie Order (Stale Order)

**Table**: `purchasing.purchaseorderdetail`

**Pattern**: `is_fully_received = false` with a `due_date` years in the past (1000-1200 days) — the item never arrived and nobody noticed.

**Detection**: `WHERE is_fully_received = false AND due_date < NOW() - INTERVAL '365 days'`

**Correction**: feed a "Pending and Forgotten Orders" report to the purchasing team; don't auto-close the line.

### 3.9.6 "Active Deadbeat" Vendor

**Table**: `purchasing.vendor`

**Pattern**: low `credit_rating` (1-2) with `is_active = true`.

**Implementation note**: the `Vendor` schema already has a `model_validator` that blocks exactly this combination — the chaos mutates the dict **after** it passes validation (deliberately bypassing the validator), simulating someone forcing activation outside the API, directly in the database. This is the real governance test: since there's no equivalent *check constraint* in Postgres, this invalid combination is only caught if someone audits the table directly.

**Detection**: `WHERE credit_rating <= 2 AND is_active = true`

**Correction**: alert compliance immediately (this isn't a "data cleaning" problem, it's an access-control failure) and consider automatically reverting `is_active` until review.

### 3.9.7 Master Data Collision (Same CNPJ, Different Names)

**Table**: `purchasing.vendor` (`vendor_name`, `vendor_details.tax_id`)

**Pattern**: two or three vendors with different `vendor_name` sharing the same `tax_id`.

**Detection**: `GROUP BY vendor_details->>'tax_id' HAVING COUNT(DISTINCT vendor_id) > 1`

**Correction**: Entity Resolution — it's not safe to auto-delete one of the records (they may have purchase orders linked to different IDs); mark as merge candidates and resolve manually, keeping a reference `master_vendor_id`.

### 3.9.8 Skipped Process (Missing Audit Steps)

**Table**: `purchasing.purchaseorderaudit`

**Pattern**: an order's trail jumps straight from `CREATED` to `CLOSED`, skipping the mandatory intermediate `APPROVED` (or `REJECTED`).

**Detection**:
```sql
SELECT purchaseorderheader_id
FROM purchasing.purchaseorderaudit
GROUP BY purchaseorderheader_id
HAVING COUNT(*) < 2
   OR NOT bool_or(action IN ('approved', 'rejected'));
```

**Correction**: flag `process_flag = 'skipped_approval'` and block any subsequent action on the order (e.g. payment) until the approval step is retroactively recorded or the order is cancelled.

### 3.9.9 Phantom Approver

**Table**: `purchasing.purchaseorderaudit`

**Pattern**: an `APPROVED`/`CLOSED` event with an `employee_id` that doesn't exist in `humanresources.employee`, or that exists but doesn't hold a management/director role.

**Detection**:
```sql
SELECT a.audit_id
FROM purchasing.purchaseorderaudit a
LEFT JOIN humanresources.employee e ON e.employee_id = a.employee_id
LEFT JOIN humanresources.jobrole j ON j.jobrole_id = e.jobrole_id
WHERE a.action IN ('approved', 'closed')
  AND (e.employee_id IS NULL OR j.job_title !~* 'gerente|manager|diretor|director|chief');
```

**Correction**: block the approval at the source (validation in the service layer before writing) — this shouldn't even reach the database; if it already has, it's a sign of an API bypass (same class of problem as 3.9.6).

---

## 3.10 Sales Anomalies — Order, Eventlog, Return Order, and Seller (1-5%)

**Bugs fixed in this section**:
- `sales_order.py` imported `ProductModel` from `api.models.product.productmodel` (a type table) instead of `Product` from `api.models.product.product` to look up `product_id` — same bug class already seen in purchasing.
- `sales_seller.py` imported `CountryRegionModel` from `api.models.person.countryregion` (no underscore) — nonexistent module; the real one is `api.models.person.country_region`.
- `sales_eventlog.py` instantiated `SalesOrderEventLogSchema(event_id=None, ...)`, but the schema required a mandatory `event_id: int` (`gt=0`) — every generated event raised a `ValidationError`. Fixed to `Optional[int] = Field(None, gt=0)`.

**Fields added** (were missing for the rules to make sense):
- `sales.salesorder.order_status` now varies (`open`/`finalized`/`partial_shipped`/`partial_payment`/`canceled`) instead of always being a fixed `"open"`.
- `person.seller` (via `sales_seller.py`) now populates `demographics.metrics.average_rating`, `total_gross_revenue`, and `chargeback_rate`, which didn't exist in the clean generation.

### 3.10.1 Broken Math (Phantom Discount)

**Table**: `sales.salesorder` (`total_amount` vs the sum of `order_items`)

**Pattern**: `total_amount` ends up well below the sum of `unit_price * quantity` across the items — simulating a discount applied directly at the header level without prorating across the lines.

**Detection**: compare `total_amount` against `SUM((item->>'unit_price')::numeric * (item->>'quantity')::int)` extracted from `order_items`.

**Correction**: never auto-recompute `total_amount` (it may have been a legitimate discount) — flag `financial_flag = 'total_mismatch'` and alert finance to investigate the source of the discount.

### 3.10.2 Schizophrenic Payment

**Table**: `sales.salesorder` (`payment_info`)

**Pattern**: `is_split = false` but `transactions` has 3 different payments.

**Detection**: `WHERE (payment_info->>'is_split')::boolean = false AND jsonb_array_length(payment_info->'transactions') > 1`

**Correction**: fix `is_split` to `true` in Silver when `transactions` has more than 1 item (it's a derivable field, not an independent source of truth).

### 3.10.3 Schrödinger's Order

**Table**: `sales.salesorder` (`order_status` vs `payment_info.transactions[].status`)

**Pattern**: `order_status = 'finalized'` but the payment transaction is `unpaid` or `refunded`.

**Detection**: `WHERE order_status = 'finalized' AND payment_info->'transactions'->0->>'status' IN ('unpaid', 'refunded')`

**Correction**: block this order from Gross Revenue calculations until resolved — a "finalized" order with no confirmed payment is accounting-invalid; never infer revenue from `order_status` alone.

### 3.10.4 Logistics Time Travel

**Table**: `sales.ordereventlog` (`event_date`)

**Pattern**: a later event in the logical sequence (e.g. `delivered`) arrives with an `event_date` earlier than an earlier event for the same order (e.g. `item_picked`) — a warehouse scanner with no internet connection that uploaded its logs late.

**Detection**:
```sql
SELECT e1.salesorder_id, e1.event_type, e1.event_date, e2.event_type, e2.event_date
FROM sales.ordereventlog e1
JOIN sales.ordereventlog e2 ON e1.salesorder_id = e2.salesorder_id AND e1.event_id < e2.event_id
WHERE e1.event_date > e2.event_date;
```

**Correction**: the Silver layer should order events by `event_date` (real time of occurrence), not by arrival order/`event_id` — reconstruct the correct timeline before any delivery-SLA calculation.

### 3.10.5 GPS Teleportation

**Table**: `sales.ordereventlog` (`event_data.gps_location`)

**Pattern**: coordinates at `(0.0, 0.0)` ("Null Island", in the Gulf of Guinea) or physically impossible (`lat: 999.9`).

**Detection**: `WHERE (event_data->'gps_location'->>'lat')::float NOT BETWEEN -90 AND 90 OR (event_data->'gps_location'->>'long')::float NOT BETWEEN -180 AND 180 OR (event_data->'gps_location'->>'lat' = '0' AND event_data->'gps_location'->>'long' = '0')`

**Correction**: discard the coordinate (don't use it in route/distance calculations) and flag `gps_flag = 'invalid'`; it's not safe to interpolate a position from neighboring events without additional context.

### 3.10.6 Infinite Refund

**Table**: `sales.returnorder` (`refund_amount`)

**Pattern**: `refund_amount` 10x higher than expected (`unit_price * quantity` of the original item) — an agent typed an extra zero on the manual refund.

**Detection**: compare `refund_amount` against the item's value in the original order's `order_items`; flag if the ratio is > 2x.

**Correction**: never auto-adjust the value (it's an already-processed financial transaction) — flag `refund_flag = 'amount_anomaly'` for audit and possible reversal of the reversal.

### 3.10.7 Phantom Inspection

**Table**: `sales.returnorder` (`inspection_data`)

**Pattern**: `return_status = 'refunded'` and `is_restockable = true`, but `inspector_user_id` and `technical_notes` (and `inspection_date`, already omitted by default in the seeder) are null — someone released the refund and restocking without examining the product.

**Detection**: `WHERE return_status = 'refunded' AND (inspection_data->>'is_restockable')::boolean = true AND inspection_data->>'inspector_user_id' IS NULL`

**Correction**: block `is_restockable = true` without a filled-in `inspector_user_id` at the service layer (a business rule, not just detection); for data already written, revert `is_restockable` to `false` until a retroactive inspection.

### 3.10.8 Toxic Partner (High-Risk Seller)

**Table**: `person.seller` (`demographics.metrics`, `is_active`)

**Pattern**: `chargeback_rate` between 0.5 and 0.9 and `average_rating` between 1.0 and 1.5, but `is_active = true`.

**Detection**: `WHERE (demographics->'metrics'->>'chargeback_rate')::numeric > 0.3 AND (demographics->'metrics'->>'average_rating')::numeric < 2.0 AND is_active = true`

**Correction**: a "High-Risk Sellers Operating" view for the risk team to monitor continuously — don't auto-deactivate (there may be a contract/negotiation in progress), but alert daily while the combination persists.

### 3.10.9 Dirty Bank Branch and Account Number

**Table**: `person.seller` (`demographics.bank_account`)

**Pattern**: `account_number` with a non-numeric character (`"12345-X"`) or an empty `agency` (`""`).

**Detection**: `WHERE demographics->'bank_account'->>'account_number' !~ '^[0-9-]+$' OR demographics->'bank_account'->>'agency' = ''`

**Correction**: normalize/validate before any payout attempt — never let the payments team try to transfer to an invalid-format account; block the payout and notify the seller to fix their registration.

---

## 3.11 Warehouse Anomalies — Transaction History, Inventory, and Location (1-5%)

**Bugs fixed in this section** (code review done by the user before the chaos work, additional findings during implementation):
- `TransactionHistorySchema.validate_transaction_logic` blocked `quantity < 0` with `raise ValueError`, contradicting the field's own description ("Positive for Inbound, Negative for Outbound") — every stock outbound (sale, negative adjustment) was rejected by Pydantic. Removed the sign check, kept only the `quantity == 0` check.
- `LocationCapabilities.warehouse_type: str = WarehouseType` assigned the Enum **class** itself as the default value (not a member of it) to a field typed as `str`. Fixed to `warehouse_type: WarehouseType = Field(default=WarehouseType.DRY)`.
- `transaction_history.py` imported `ProductModel` from `api.models.product.product` (the real class is `Product`) and `TransactionTypeModel` from `api.models.warehouse.transactiontype` (the real class is `WarehouseTransactionType`) — two guaranteed `ImportError`s.
- `product_inventory.py` used `random.randint`/`random.choice` without ever importing `random` (guaranteed `NameError`), **and** imported `ProductModel` instead of `Product` (same bug as `transaction_history.py`).
- `ProductInventorySchema.inventory_id` required a mandatory `int`, but the seeder passes `inventory_id=None` (autoincrement) — the same bug class already fixed in `SalesOrderEventLogSchema.event_id` (section 3.10). Fixed to `Optional[int]`.

### 3.11.1 Cold Chain Break (IoT Glitch)

**Table**: `warehouse.transactionhistory` (`traceability_info.iot_sensor_data.temp_at_scan`)

**Pattern**: a transaction involving a `cold_storage` location records `temp_at_scan = 45.0`.

**Detection**:
```sql
SELECT t.transaction_id
FROM warehouse.transactionhistory t
JOIN warehouse.warehouselocation wl
  ON wl.warehouselocation_id IN (
       (t.traceability_info->>'source_warehouselocation_id')::int,
       (t.traceability_info->>'target_warehouselocation_id')::int
     )
WHERE wl.capabilities->>'warehouse_type' = 'cold_storage'
  AND (t.traceability_info->'iot_sensor_data'->>'temp_at_scan')::float > 8.0;
```

**Correction**: a dedicated Compliance pipeline that fires a "Batch Compromised by Cold Chain Break" alert in real time (not in an overnight batch) — a product potentially unfit for consumption/use can't wait for the next Data Lake run.

### 3.11.2 Logistics Black Hole

**Table**: `warehouse.transactionhistory` (`traceability_info.target_warehouselocation_id`)

**Pattern**: a transfer (type with `action_effect = 'neutral'`) has `source_warehouselocation_id` filled in, but `target_warehouselocation_id` null — the load left one warehouse and never arrived anywhere on record.

**Detection**:
```sql
SELECT t.transaction_id
FROM warehouse.transactionhistory t
JOIN warehouse.transactiontype tt ON tt.transactiontype_id = t.transactiontype_id
WHERE tt.action_effect = 'neutral'
  AND t.traceability_info->>'source_warehouselocation_id' IS NOT NULL
  AND t.traceability_info->>'target_warehouselocation_id' IS NULL;
```

**Correction**: alert operations/asset security immediately (could be cargo theft, not just a system failure) — never auto-infer a destination.

### 3.11.3 Perishable Zombie

**Table**: `warehouse.productinventory` (`quantity`, `is_active`, `attributes.expiration_date`)

**Pattern**: high `quantity` (3000-8000), `is_active = true`, but `expiration_date` expired 700-900 days ago.

**Detection**: `WHERE is_active = true AND quantity > 0 AND (attributes->>'expiration_date')::date < CURRENT_DATE - INTERVAL '30 days'`

**Correction**: an automatic write-off routine isn't safe (there could be a data-entry error on the expiration date) — flag `inventory_flag = 'expired_active_stock'` and exclude it from financial stock valuation (cost/CEO report) until physically confirmed, but without deleting the record.

### 3.11.4 Quantum Batch

**Table**: `warehouse.productinventory` (`attributes.lot_number`, `attributes.manufacturing_date`)

**Pattern**: a product with `quantity > 0` but `lot_number` and `manufacturing_date` null — a traceability break on items that normally require it (food, pharmaceuticals, warrantied electronics).

**Detection**: `WHERE quantity > 0 AND attributes->>'lot_number' IS NULL AND attributes->>'manufacturing_date' IS NULL`

**Correction**: block the sale/shipment of this item until the batch is retroactively identified — it's not safe to dispatch a product with no traceability in regulated categories.

### 3.11.5 North Pole in the Desert

**Table**: `warehouse.warehouselocation` (`capabilities.warehouse_type` vs `capabilities.temperature_range`)

**Pattern**: `warehouse_type = 'cold_storage'` but `temperature_range = {min_temp: 25.0, max_temp: 40.0}` (a hot-chamber range).

**Detection**: `WHERE capabilities->>'warehouse_type' = 'cold_storage' AND (capabilities->'temperature_range'->>'min_temp')::float > 0`

**Correction**: an inconsistent registration blocks operation — alert the facilities/registration team; the Silver layer should flag `location_flag = 'temperature_mismatch'` and prevent "cold chain" products from being routed to this location until manually corrected.

### 3.11.6 Geographic Teleportation

**Table**: `warehouse.warehouselocation` (`geo_location.postal_code` vs `geo_location.country_name`)

**Pattern**: a CEP in another country's format (e.g. an American `"90210"`) on a record with a different country.

**Detection**: validate `postal_code` against the format expected for `country_name`/`country_id` (per-country regex); flag any divergence.

**Correction**: it's not safe to plot this location on a BI map without correction — exclude it from geographic visualizations until the CEP is validated/corrected, flagging `geo_flag = 'postal_code_country_mismatch'`.

---

## 4. Implementation in the Seeders

### 4.1 Generic Chaos Engineering Function

```python
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta

def apply_data_quality_chaos(
    batch: List[Dict[str, Any]],
    chaos_rate: float = 0.05,
    allowed_chaos_types: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Applies chaos engineering (5% default) to the generated data.

    Args:
        batch: List of dicts with the data to be inserted
        chaos_rate: Percentage of records to affect (0.01 to 0.10)
        allowed_chaos_types: List of allowed chaos types (None = all)

    Returns:
        List of dicts with injected anomalies
    """
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    all_chaos_types = [
        "duplicate", "dirty_cpf", "dirty_phone", "dirty_email",
        "dirty_cep", "dirty_name", "outlier_value", "outlier_quantity",
        "fraud_fake_cpf", "fraud_fake_address", "fraud_cloned_card",
        "fraud_duplicate_order", "fraud_phantom_vendor",
        "incomplete_payment", "incomplete_shipping", "incomplete_delivery",
        "warehouse_lost_transfer", "warehouse_quantity_mismatch",
        "warehouse_negative_stock", "warehouse_missing_location",
        "warehouse_quality_fail", "warehouse_cold_chain_break",
        "negative_values", "missing_refs"
    ]

    chaos_types = allowed_chaos_types if allowed_chaos_types else all_chaos_types

    for idx in chaos_indices:
        chaos_type = random.choice(chaos_types)

        # Duplication
        if chaos_type == "duplicate" and "customer_name" in batch[idx]:
            duplicated = batch[idx].copy()
            duplicated["customer_name"] = duplicated["customer_name"].upper()
            duplicated["row_guid"] = uuid.uuid4()
            batch.append(duplicated)

        # Fake CPF
        elif chaos_type == "dirty_cpf" and "cpf" in batch[idx]:
            batch[idx]["cpf"] = random.choice([
                "000.000.000-00", "111.111.111-11", "123.456.789-00"
            ])

        # Invalid phone
        elif chaos_type == "dirty_phone" and "phone" in batch[idx]:
            batch[idx]["phone"] = random.choice([
                "0000000", "11111111111", "(00) 0000-0000", "99 99999-9999"
            ])

        # Invalid email
        elif chaos_type == "dirty_email" and "email" in batch[idx]:
            batch[idx]["email"] = random.choice([
                "email@dominio", "@dominio.com", "email@@dominio.com", "test@test.test"
            ])

        # Invalid CEP
        elif chaos_type == "dirty_cep" and "cep" in batch[idx]:
            batch[idx]["cep"] = random.choice([
                "00000-000", "XXXXX-XXX", "12345-678"
            ])

        # Invalid name
        elif chaos_type == "dirty_name" and "customer_name" in batch[idx]:
            batch[idx]["customer_name"] = random.choice([
                "João123", "Maria###", "Cliente Teste", "XXXX", "   ", "A"
            ])

        # Value outlier
        elif chaos_type == "outlier_value" and "total_amount" in batch[idx]:
            current_value = batch[idx]["total_amount"]
            batch[idx]["total_amount"] = current_value * random.randint(5, 10)

        # Quantity outlier
        elif chaos_type == "outlier_quantity" and "quantity" in batch[idx]:
            batch[idx]["quantity"] = random.randint(1000, 10000)

        # Fraud: fake CPF with a real name
        elif chaos_type == "fraud_fake_cpf" and "cpf" in batch[idx]:
            batch[idx]["cpf"] = "000.000.000-00"
            # Name stays realistic

        # Fraud: fake name with a real address
        elif chaos_type == "fraud_fake_address" and "customer_name" in batch[idx]:
            batch[idx]["customer_name"] = "Cliente Teste"
            # Address stays valid

        # Fraud: cloned card (geographically impossible purchases)
        elif chaos_type == "fraud_cloned_card" and "shipping_city" in batch[idx]:
            batch[idx]["shipping_city"] = "São Paulo"
            batch[idx]["billing_city"] = "Rio de Janeiro"
            batch[idx]["order_date"] = datetime.now()

        # Fraud: duplicate purchase order
        elif chaos_type == "fraud_duplicate_order" and "purchaseorderheader_id" in batch[idx]:
            duplicated = batch[idx].copy()
            duplicated["purchaseorderheader_id"] = f"PO-{random.randint(100000000, 999999999)}"
            duplicated["order_date"] = batch[idx]["order_date"] + timedelta(days=1)
            duplicated["row_guid"] = uuid.uuid4()
            batch.append(duplicated)

        # Incomplete process: payment pending for a long time
        elif chaos_type == "incomplete_payment" and "order_status" in batch[idx]:
            batch[idx]["order_status"] = "pending"
            batch[idx]["order_date"] = datetime.now() - timedelta(days=45)

        # Incomplete process: paid but not shipped
        elif chaos_type == "incomplete_shipping" and "payment_info" in batch[idx]:
            batch[idx]["payment_info"]["transactions"][0]["status"] = "paid"
            batch[idx]["shipping_status"] = None
            batch[idx]["order_date"] = datetime.now() - timedelta(days=20)

        # Warehouse: lost transfer
        elif chaos_type == "warehouse_lost_transfer" and "transaction_date" in batch[idx]:
            batch[idx]["transaction_date"] = datetime.now() - timedelta(days=95)
            if "traceability_info" in batch[idx]:
                batch[idx]["traceability_info"]["status"] = "in_transit"

        # Warehouse: negative stock
        elif chaos_type == "warehouse_negative_stock" and "quantity" in batch[idx]:
            batch[idx]["quantity"] = random.randint(-50, -1)

        # Warehouse: no physical location
        elif chaos_type == "warehouse_missing_location" and "warehouselocation_id" in batch[idx]:
            batch[idx]["warehouselocation_id"] = None

        # Warehouse: failed QC but shipped
        elif chaos_type == "warehouse_quality_fail" and "traceability_info" in batch[idx]:
            batch[idx]["traceability_info"]["quality_check_status"] = "fail"
            # But reference_order_id stays filled in (product was sold)

        # Warehouse: broken cold chain
        elif chaos_type == "warehouse_cold_chain_break" and "traceability_info" in batch[idx]:
            if "iot_sensor_data" in batch[idx]["traceability_info"]:
                batch[idx]["traceability_info"]["iot_sensor_data"]["temp_at_scan"] = random.uniform(15.0, 25.0)

        # Negative values
        elif chaos_type == "negative_values":
            if "actual_cost" in batch[idx]:
                batch[idx]["actual_cost"] = -abs(batch[idx]["actual_cost"])
            elif "total_due" in batch[idx]:
                batch[idx]["total_due"] = -abs(batch[idx]["total_due"])
            elif "freight" in batch[idx]:
                batch[idx]["freight"] = -abs(batch[idx]["freight"])

        # Missing references
        elif chaos_type == "missing_refs":
            if "reference_order_id" in batch[idx]:
                batch[idx]["reference_order_id"] = None
            elif "vendor_id" in batch[idx]:
                batch[idx]["vendor_id"] = None

    return batch
```

### 4.2 Usage Example in a Seeder

```python
# In dbapi/api/seeder/sales/sales_order.py

async def seed_sales_orders(count: int = 50):
    async with AsyncSessionLocal() as session:
        # 1. Fetch FKs
        customer_ids = (await session.execute(select(CustomerModel.customer_id))).scalars().all()
        # ... other FKs

        # 2. Generate clean data
        orders = generate_sales_order_payloads(count, customer_ids, channel_ids, offer_ids, seller_ids, product_ids)

        # 3. Apply chaos engineering (5% anomalies)
        orders_with_chaos = apply_data_quality_chaos(
            orders,
            chaos_rate=0.05,
            allowed_chaos_types=[
                "duplicate", "dirty_cpf", "fraud_fake_cpf",
                "fraud_cloned_card", "incomplete_payment", "negative_values"
            ]
        )

        # 4. Bulk insert
        stmt = insert(SalesOrderModel).values(orders_with_chaos)
        await session.execute(stmt)
        await session.commit()

        print(f"✅ {len(orders_with_chaos)} orders seeded (including {int(len(orders_with_chaos) * 0.05)} with anomalies)")
```

---

## 5. Metadata and Documentation

For each table that receives chaos-engineered data, document:

1. **Injected anomaly types** (which chaos_types are active)
2. **Anomaly percentage** (configured chaos_rate)
3. **Detection queries** (SQL to identify each anomaly type)
4. **Test scenarios** (expected cleaning pipelines)

### Metadata Example (JSON)

```json
{
  "table": "sales.salesorder",
  "chaos_engineering": {
    "enabled": true,
    "chaos_rate": 0.05,
    "chaos_types": [
      {
        "type": "duplicate",
        "description": "Duplicate records with case variation",
        "detection_query": "SELECT customer_id, UPPER(customer_name) as name_upper, COUNT(*) FROM sales.salesorder GROUP BY customer_id, name_upper HAVING COUNT(*) > 1",
        "expected_count": "~2-5% of total records"
      },
      {
        "type": "fraud_cloned_card",
        "description": "Geographically impossible purchases on the same day",
        "detection_query": "WITH orders AS (SELECT customer_id, order_date::date, shipping_city, LAG(shipping_city) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_city FROM sales.salesorder) SELECT * FROM orders WHERE shipping_city != prev_city AND order_date = prev_order_date",
        "expected_count": "~1-2% of total records"
      }
    ]
  }
}
```

---

## 6. Detection Queries for Data Quality Dashboards

### 6.1 Data Quality Dashboard

```sql
-- Total records with invalid CPF
SELECT
    'Invalid CPF' as issue_type,
    COUNT(*) as issue_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person.customer) as percentage
FROM person.customer
WHERE cpf IN ('000.000.000-00', '111.111.111-11', '123.456.789-00');

-- Duplicate records (case-insensitive)
SELECT
    'Duplicates' as issue_type,
    COUNT(*) as issue_count
FROM (
    SELECT UPPER(customer_name), COUNT(*) as cnt
    FROM person.customer
    GROUP BY UPPER(customer_name)
    HAVING COUNT(*) > 1
) subq;

-- Negative stock
SELECT
    'Negative Stock' as issue_type,
    COUNT(*) as issue_count
FROM warehouse.product_inventory
WHERE quantity_on_hand < 0;

-- Orders paid but not shipped (> 15 days)
SELECT
    'Unpaid Orders' as issue_type,
    COUNT(*) as issue_count
FROM sales.salesorder
WHERE order_status = 'paid'
  AND shipping_status IS NULL
  AND order_date < NOW() - INTERVAL '15 days';
```

### 6.2 Quality Dashboard — Human Resources

```sql
-- Salary outside the role's range (join with the current jobrole)
SELECT
    'Salary Out of Range' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j
  ON e.job_role_id = j.jobrole_id AND j.is_current = true
WHERE e.current_salary NOT BETWEEN j.min_salary AND j.max_salary;

-- Salespeople with no registered commission
SELECT
    'Sales Without Commission' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j ON e.job_role_id = j.jobrole_id
WHERE j.job_title ILIKE '%sales%'
  AND COALESCE((e.role_attributes->>'comission_pct')::numeric, 0) = 0;

-- Salespeople with no territory/region
SELECT
    'Sales Without Territory' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j ON e.job_role_id = j.jobrole_id
WHERE j.job_title ILIKE '%sales%'
  AND (e.role_attributes->'territory_codes' IS NULL
       OR jsonb_array_length(e.role_attributes->'territory_codes') = 0);

-- Broken SCD2 records (more than one "current" per employee)
SELECT employee_id, COUNT(*) as current_versions
FROM humanresources.employee
WHERE is_current = true
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

### 6.3 Fraud Detection Dashboard

```sql
-- Multiple registrations with the same email
SELECT
    email,
    COUNT(DISTINCT cpf) as distinct_cpfs,
    COUNT(*) as total_accounts
FROM person.customer_email
GROUP BY email
HAVING COUNT(DISTINCT cpf) > 2;

-- Geographically impossible purchases
WITH order_pairs AS (
    SELECT
        customer_id,
        order_date,
        shipping_city,
        LAG(shipping_city) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_city,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_date
    FROM sales.salesorder
)
SELECT *
FROM order_pairs
WHERE shipping_city != prev_city
  AND order_date::date = prev_date::date;

-- Phantom vendors (no deliveries in the last 90 days)
SELECT
    v.vendor_id,
    v.vendor_name,
    MAX(poh.order_date) as last_order_date,
    DATEDIFF('day', MAX(poh.order_date), NOW()) as days_since_last_order
FROM purchasing.vendor v
LEFT JOIN purchasing.purchaseorderheader poh ON v.vendor_id = poh.vendor_id
GROUP BY v.vendor_id, v.vendor_name
HAVING MAX(poh.order_date) < NOW() - INTERVAL '90 days'
   OR MAX(poh.order_date) IS NULL;
```

---

## 7. Next Steps

1. **Implement `apply_data_quality_chaos()` in a utility module**:
   - Create `dbapi/api/utils/chaos_engineering.py` with the generic function
   - Import it into every seeder in the critical schemas
   - ✅ Done for `marketing`: the functions live in `dbapi/api/seeder/chaos/marketing_chaos.py`, re-exported by `dbapi/api/seeder/chaos/__init__.py` and imported by all 4 seeders (`from api.seeder.chaos import apply_campaign_chaos, ...`). Use this folder as the standard for the remaining domains (`dbapi/api/seeder/chaos/<domain>_chaos.py`).
   - ✅ Done for `person`: `dbapi/api/seeder/chaos/person_chaos.py` (`apply_customer_chaos`, `apply_customer_email_chaos`, `apply_geography_code_chaos`, `apply_false_foreigner_chaos`), imported by `person_customer.py`, `person_customer_email.py`, `person_state_province.py`, and `person_country.py`.
   - ✅ Done for `product`: `dbapi/api/seeder/chaos/product_chaos.py` (`apply_product_chaos`, `apply_category_chaos`, `apply_embedding_chaos`), imported by `product.py`, `product_category.py`, and `product_embedding.py`. Added the new `product.sell_start_date`/`sell_end_date` and `productcategory.parent_category_id` columns (see section 3.8).
   - ✅ Done for `purchasing`: `dbapi/api/seeder/chaos/purchasing_chaos.py` (`apply_purchaseorderheader_chaos`, `apply_purchaseorderdetail_chaos`, `apply_vendor_chaos`, `apply_purchase_audit_chaos`), imported by all 4 seeders in the domain. Fixed the broken imports that prevented any of the 4 seeders from running, the `status == 4` bug, and added the entry point (`seed_purchase_order_audit`) that was missing from `purchase_purchaseorderaudit.py` (see section 3.9).
   - ✅ Done for `sales`: `dbapi/api/seeder/chaos/sales_chaos.py` (`apply_sales_order_chaos`, `apply_eventlog_chaos`, `apply_return_order_chaos`, `apply_seller_chaos`), imported by all 4 seeders in the domain. Fixed broken imports (`ProductModel`→`Product`, `countryregion`→`country_region`) and a guaranteed `ValidationError` in `sales_eventlog.py` (mandatory `event_id` receiving `None`). Note: `apply_eventlog_chaos` operates on already-built `SalesOrderEventLogSchema` instances, not `dict`s — it's the only chaos function in the project that mutates Pydantic object attributes directly, instead of post-`model_dump()` dicts (see section 3.10).
   - ✅ Done for `warehouse`: `dbapi/api/seeder/chaos/warehouse_chaos.py` (`apply_transaction_history_chaos`, `apply_inventory_chaos`, `apply_warehouse_location_chaos`), imported by all 3 seeders in the domain. Fixed 2 Pydantic schema bugs found via the user's code review (`TransactionHistorySchema` blocked negative `quantity`, contradicting its own description; `LocationCapabilities.warehouse_type` assigned the Enum class instead of one of its members), plus broken imports (`ProductModel`→`Product`, `TransactionTypeModel`→`WarehouseTransactionType`), `product_inventory.py` missing `import random`, and `ProductInventorySchema.inventory_id` with the same mandatory `event_id`/`gt=0` bug receiving `None` (see section 3.11).

2. **Update existing seeders**:
   - `sales/sales_order.py`
   - `purchasing/purchase_purchaseorderheader.py`
   - `warehouse/transaction_history.py`
   - `person/person_customer.py`

3. **Add validations to the product seeder**:
   - Check the count before inserting (100k limit)
   - Add argparse to control chaos_rate

4. **Implement Data Quality dashboards**:
   - Integrate with Trino for analytical queries
   - Create materialized views with quality metrics
   - Airflow alerts when anomalies exceed thresholds

5. **Document chaos metadata**:
   - Create a JSON file per table in `dbapi/api/seeder/metadata/`
   - Consume the metadata in validation pipelines (Great Expectations)

6. **Test cleaning pipelines**:
   - Implement dbt models for normalization
   - Create unit tests for fraud detection
   - Validate data recovery after cleaning

7. **Fix the root cause of the missing history in `humanresources.employee`**:
   - Today the seeder always writes `is_current=true` without closing previous versions (see [3.5.5](#355-broken-scd2--multiple-current-records-or-temporal-gaps))
   - Implement upsert/MERGE by `employee_id` in the load process, comparing tracked attributes (salary, role, department) against the existing `is_current=true` version
   - Only then will the Silver layer be able to fix anomalies using the individual's real history instead of relying on reference data/peer statistics

8. **Future architecture: Silver Corrections + Quarantine + Great Expectations** (planned, not yet implemented — recorded here as a starting point):
   - **Where it runs**: Spark, in the same style as the jobs already in `spark/jobs/silver_layer_advworks_scripts/`. No new service — Great Expectations goes in as one more step inside the Silver job itself, using GX's Spark execution engine.
   - **Pipeline per domain**: (1) reads Bronze; (2) runs a suite of Expectations — each rule already documented in sections 3.5 to 3.11 of this file (with a ready-made "Detection" query) becomes an `Expectation`; (3) rows that pass move on to normal Silver transformation; (4) rows that fail go to a **quarantine table** (same structure + audit columns `dq_rule_id`, `dq_failure_reason`, `quarantined_at`), living in the same Iceberg lakehouse as Silver (e.g. `nexomart.quarantine.<table>`), not in Postgres — that way Trino can already see it without needing another source.
   - **Correction job**: a registry of functions (same spirit as `dbapi/api/seeder/chaos/`, but for correction instead of injection) — one function per rule, each with an `auto_fix` policy (objectively safe normalizations: trim, uppercase, cast, clamp) or `flag_only` (sensitive financial data — never auto-correct, only flag for human review, per the policy already described in each 3.x section of this document). Reads from quarantine, applies the correction when `auto_fix=True`, and does a **MERGE** (never append) back into Silver, "promoting" the row.
   - **Late-arriving / dynamic backfill**: the MERGE key is always the table's natural key (e.g. `(ad_id, date_ref)` in marketing, not a load ID), and the job runs over a **configurable moving window** (e.g. the last 7-15 days), never just "yesterday". This makes the same routine job also work as automatic backfill when a correction/late-arriving record shows up — no separate "backfill mode" needed.
   - **Agreed initial scope**: prototype this full chain (read → GX → quarantine → correction → MERGE) end to end in **one domain first** (candidate: `marketing`, since it already has the most detailed detection queries in section 3.6) before generalizing to the other 5 domains.
   - **Open, not blocking the start**: whether the post-Silver path is Gold → Trino → Postgres (for Power BI) or Power BI connecting directly to Gold. That decision is independent of the Bronze→Silver+quarantine work above.

---

## Conclusion

This document serves as the **single source of truth** for all anomaly types injected into the data. It must be kept up to date whenever new chaos types are added to the seeders.
