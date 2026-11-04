"""
Chaos Engineering Utilities for Data Quality Testing

This module provides functions to inject controlled anomalies into seeded data
to simulate real-world data quality issues, fraud patterns, and operational failures.

These anomalies are intentional and documented, designed for:
- Testing data cleaning pipelines
- Training fraud detection models
- Validating data quality rules
- Preparing analysts for production scenarios
"""

import random
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

# ==================== CHAOS STRATEGIES ====================
# Each function receives a record and returns None or a new record (for duplications)


def chaos_duplicate(record: dict[str, Any]) -> dict[str, Any] | None:
    """Duplicates a record with case, whitespace, or accent variation."""
    if "customer_name" not in record:
        return None

    duplicated = record.copy()
    variation = random.choice(
        [
            lambda s: s.upper(),
            lambda s: s.lower(),
            lambda s: f"  {s}  ",
            lambda s: s.replace("ã", "a").replace("ç", "c").replace("á", "a").replace("é", "e"),
        ]
    )
    duplicated["customer_name"] = variation(duplicated["customer_name"])
    duplicated["row_guid"] = uuid.uuid4()
    return duplicated


def chaos_dirty_cpf(record: dict[str, Any]) -> None:
    """Injects an invalid CPF."""
    if "cpf" in record:
        record["cpf"] = random.choice(
            ["000.000.000-00", "111.111.111-11", "123.456.789-00", "12345678900", "999.999.999-99"]
        )


def chaos_dirty_phone(record: dict[str, Any]) -> None:
    """Injects an invalid phone number."""
    if "phone" in record:
        record["phone"] = random.choice(
            ["0000000", "11111111111", "(00) 0000-0000", "99 99999-9999", "+55 11 91234-567"]
        )


def chaos_dirty_email(record: dict[str, Any]) -> None:
    """Injects an invalid email."""
    if "email" in record:
        record["email"] = random.choice(
            [
                "email@domain",
                "@domain.com",
                "email@@domain.com",
                "email@domain .com",
                "test@test.test",
            ]
        )


def chaos_dirty_cep(record: dict[str, Any]) -> None:
    """Injects an invalid postal code (CEP)."""
    if "cep" in record:
        record["cep"] = random.choice(["00000-000", "XXXXX-XXX", "12345-678", "01310100"])


def chaos_dirty_name(record: dict[str, Any]) -> None:
    """Injects an invalid name."""
    if "customer_name" in record:
        record["customer_name"] = random.choice(
            ["João123", "Maria###", "Cliente Teste", "XXXX", "   ", "A"]
        )


def chaos_outlier_value(record: dict[str, Any]) -> None:
    """Multiplies a value by 5-10x (legitimate outlier)."""
    if "total_amount" in record:
        multiplier = random.randint(5, 10)
        if isinstance(record["total_amount"], Decimal):
            record["total_amount"] = record["total_amount"] * multiplier
        else:
            record["total_amount"] = float(record["total_amount"]) * multiplier


def chaos_outlier_quantity(record: dict[str, Any]) -> None:
    """Injects an abnormally high quantity."""
    if "quantity" in record:
        record["quantity"] = random.randint(1000, 10000)


def chaos_outlier_lead_time(record: dict[str, Any]) -> None:
    """Injects a very long lead time (90-180 days)."""
    if "ship_date" in record and "order_date" in record:
        record["ship_date"] = record["order_date"] + timedelta(days=random.randint(90, 180))


def chaos_fraud_fake_cpf(record: dict[str, Any]) -> None:
    """Fake CPF with a realistic name (fraud)."""
    if "cpf" in record:
        record["cpf"] = "000.000.000-00"


def chaos_fraud_fake_address(record: dict[str, Any]) -> None:
    """Fake name with a real address (fraud)."""
    if "customer_name" in record:
        record["customer_name"] = random.choice(
            ["Cliente Teste", "Fulano de Tal", "João Silva", "Maria Santos"]
        )


def chaos_fraud_cloned_card(record: dict[str, Any]) -> None:
    """Geographically impossible purchases (cloned card)."""
    if "shipping_city" in record:
        record["shipping_city"] = "São Paulo"
        if "billing_city" in record:
            record["billing_city"] = "Rio de Janeiro"
        if "order_date" in record:
            record["order_date"] = datetime.now()


def chaos_fraud_high_value_suspicious(record: dict[str, Any]) -> None:
    """High value with suspicious data (free email + new registration)."""
    if "total_amount" in record:
        record["total_amount"] = Decimal(random.uniform(50000.0, 100000.0)).quantize(
            Decimal("0.01")
        )
    if "email" in record:
        record["email"] = f"test{random.randint(1, 999)}@gmail.com"


def chaos_fraud_multiple_cards_same_address(record: dict[str, Any]) -> None:
    """Multiple cards at the same address (fraud pattern)."""
    # Implementation depends on cross-record logic (not applicable here, marker only)
    pass


def chaos_fraud_duplicate_order(record: dict[str, Any]) -> dict[str, Any] | None:
    """Duplicates a purchase order (possible bribery or error)."""
    if "purchaseorderheader_id" not in record:
        return None

    duplicated = record.copy()
    duplicated["purchaseorderheader_id"] = f"PO-{random.randint(100000000, 999999999)}"
    if "order_date" in duplicated:
        duplicated["order_date"] = record["order_date"] + timedelta(days=1)
    duplicated["row_guid"] = uuid.uuid4()
    return duplicated


def chaos_fraud_phantom_vendor(record: dict[str, Any]) -> None:
    """Phantom vendor (invalid CNPJ)."""
    if "vendor_name" in record:
        record["vendor_name"] = f"Fornecedor Fantasma {random.randint(1, 999)}"
    if "cnpj" in record:
        record["cnpj"] = "00.000.000/0000-00"


def chaos_fraud_overpriced(record: dict[str, Any]) -> None:
    """Overbilling (price 3-8x above normal)."""
    if "unit_price" in record:
        multiplier = random.randint(3, 8)
        if isinstance(record["unit_price"], Decimal):
            record["unit_price"] = record["unit_price"] * multiplier
        else:
            record["unit_price"] = float(record["unit_price"]) * multiplier


def chaos_fraud_unauthorized_approval(record: dict[str, Any]) -> None:
    """Approval without authorization (marker, validated at the API level)."""
    # Validation should be checked against the permissions table
    pass


def chaos_fraud_personal_delivery(record: dict[str, Any]) -> None:
    """Delivery to a personal address instead of a warehouse (marker)."""
    # Validation should check whether the address is in warehouselocation
    pass


def chaos_incomplete_payment(record: dict[str, Any]) -> None:
    """Payment pending for a long time (> 30 days)."""
    if "order_status" in record:
        record["order_status"] = "pending"
    if "order_date" in record:
        record["order_date"] = datetime.now() - timedelta(days=random.randint(30, 90))


def chaos_incomplete_shipping(record: dict[str, Any]) -> None:
    """Paid but never shipped (> 15 days)."""
    if "payment_info" in record and "transactions" in record["payment_info"]:
        record["payment_info"]["transactions"][0]["status"] = "paid"
    if "order_date" in record:
        record["order_date"] = datetime.now() - timedelta(days=random.randint(15, 30))


def chaos_incomplete_delivery(record: dict[str, Any]) -> None:
    """Shipped but never delivered (> 90 days)."""
    if "shipping_status" in record:
        record["shipping_status"] = "in_transit"
    if "shipping_date" in record:
        record["shipping_date"] = datetime.now() - timedelta(days=random.randint(90, 180))


def chaos_incomplete_product_missing(record: dict[str, Any]) -> None:
    """Nonexistent product (improbable ID)."""
    if "product_id" in record:
        record["product_id"] = 99999


def chaos_warehouse_lost_transfer(record: dict[str, Any]) -> None:
    """Lost transfer (in transit for > 90 days)."""
    if "transaction_date" in record:
        record["transaction_date"] = datetime.now() - timedelta(days=random.randint(90, 180))
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        record["traceability_info"]["status"] = "in_transit"


def chaos_warehouse_quantity_mismatch(record: dict[str, Any]) -> None:
    """Quantity discrepancy (received < shipped)."""
    if "quantity" in record:
        reduction = random.uniform(0.05, 0.15)
        record["quantity"] = int(record["quantity"] * (1 - reduction))


def chaos_warehouse_negative_stock(record: dict[str, Any]) -> None:
    """Negative stock (overselling)."""
    if "quantity" in record:
        record["quantity"] = random.randint(-50, -1)


def chaos_warehouse_missing_location(record: dict[str, Any]) -> None:
    """Product with no physical location."""
    if "warehouselocation_id" in record:
        record["warehouselocation_id"] = None


def chaos_warehouse_quality_fail(record: dict[str, Any]) -> None:
    """Product failed quality check but still shipped."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        record["traceability_info"]["quality_check_status"] = "fail"


def chaos_warehouse_cold_chain_break(record: dict[str, Any]) -> None:
    """Broken cold chain (temperature outside the allowed range)."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        if "iot_sensor_data" in record["traceability_info"]:
            if isinstance(record["traceability_info"]["iot_sensor_data"], dict):
                record["traceability_info"]["iot_sensor_data"]["temp_at_scan"] = random.uniform(
                    15.0, 25.0
                )


def chaos_warehouse_same_location_transfer(record: dict[str, Any]) -> None:
    """Transfer to the same location (system error)."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        if "source_warehouselocation_id" in record["traceability_info"]:
            record["traceability_info"]["target_warehouselocation_id"] = record[
                "traceability_info"
            ]["source_warehouselocation_id"]


def chaos_warehouse_missing_traceability(record: dict[str, Any]) -> None:
    """Transaction with no traceability."""
    if "traceability_info" in record:
        record["traceability_info"] = None


def chaos_negative_cost(record: dict[str, Any]) -> None:
    """Negative cost (system error)."""
    if "actual_cost" in record:
        if isinstance(record["actual_cost"], Decimal):
            record["actual_cost"] = -abs(record["actual_cost"])
        else:
            record["actual_cost"] = -abs(float(record["actual_cost"]))


def chaos_negative_quantity(record: dict[str, Any]) -> None:
    """Negative quantity (integration bug)."""
    if "quantity" in record:
        record["quantity"] = -abs(record["quantity"])


def chaos_negative_freight(record: dict[str, Any]) -> None:
    """Negative freight (poorly implemented adjustment)."""
    if "freight" in record:
        if isinstance(record["freight"], Decimal):
            record["freight"] = -abs(record["freight"])
        else:
            record["freight"] = -abs(float(record["freight"]))


def chaos_missing_order_ref(record: dict[str, Any]) -> None:
    """Movement with no originating order."""
    if "reference_order_id" in record:
        record["reference_order_id"] = None


def chaos_missing_vendor_ref(record: dict[str, Any]) -> None:
    """Purchase with no vendor (system error)."""
    if "vendor_id" in record:
        record["vendor_id"] = None


# ==================== CHAOS STRATEGY MAP ====================
CHAOS_STRATEGIES: dict[str, Callable[[dict[str, Any]], dict[str, Any] | None]] = {
    # Duplication
    "duplicate": chaos_duplicate,
    # Dirty data
    "dirty_cpf": chaos_dirty_cpf,
    "dirty_phone": chaos_dirty_phone,
    "dirty_email": chaos_dirty_email,
    "dirty_cep": chaos_dirty_cep,
    "dirty_name": chaos_dirty_name,
    # Outliers
    "outlier_value": chaos_outlier_value,
    "outlier_quantity": chaos_outlier_quantity,
    "outlier_lead_time": chaos_outlier_lead_time,
    # Purchase fraud
    "fraud_fake_cpf": chaos_fraud_fake_cpf,
    "fraud_fake_address": chaos_fraud_fake_address,
    "fraud_cloned_card": chaos_fraud_cloned_card,
    "fraud_high_value_suspicious": chaos_fraud_high_value_suspicious,
    "fraud_multiple_cards_same_address": chaos_fraud_multiple_cards_same_address,
    # Purchasing fraud
    "fraud_duplicate_order": chaos_fraud_duplicate_order,
    "fraud_phantom_vendor": chaos_fraud_phantom_vendor,
    "fraud_overpriced": chaos_fraud_overpriced,
    "fraud_unauthorized_approval": chaos_fraud_unauthorized_approval,
    "fraud_personal_delivery": chaos_fraud_personal_delivery,
    # Incomplete processes
    "incomplete_payment": chaos_incomplete_payment,
    "incomplete_shipping": chaos_incomplete_shipping,
    "incomplete_delivery": chaos_incomplete_delivery,
    "incomplete_product_missing": chaos_incomplete_product_missing,
    # Warehouse
    "warehouse_lost_transfer": chaos_warehouse_lost_transfer,
    "warehouse_quantity_mismatch": chaos_warehouse_quantity_mismatch,
    "warehouse_negative_stock": chaos_warehouse_negative_stock,
    "warehouse_missing_location": chaos_warehouse_missing_location,
    "warehouse_quality_fail": chaos_warehouse_quality_fail,
    "warehouse_cold_chain_break": chaos_warehouse_cold_chain_break,
    "warehouse_same_location_transfer": chaos_warehouse_same_location_transfer,
    "warehouse_missing_traceability": chaos_warehouse_missing_traceability,
    # Negative values
    "negative_cost": chaos_negative_cost,
    "negative_quantity": chaos_negative_quantity,
    "negative_freight": chaos_negative_freight,
    # Missing references
    "missing_order_ref": chaos_missing_order_ref,
    "missing_vendor_ref": chaos_missing_vendor_ref,
}


# ==================== MAIN FUNCTION ====================
def apply_data_quality_chaos(
    batch: list[dict[str, Any]],
    chaos_rate: float = 0.05,
    allowed_chaos_types: list[str] = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Applies chaos engineering (5% default) to the generated data using the strategy pattern.

    Args:
        batch: List of dictionaries with the data to be inserted
        chaos_rate: Percentage of records to affect (0.01 to 0.10)
        allowed_chaos_types: List of allowed chaos types (None = all available)
        verbose: If True, prints statistics about the chaos applied

    Returns:
        List of dictionaries with anomalies injected

    Example:
        >>> orders = generate_sales_order_payloads(100, ...)
        >>> orders_with_chaos = apply_data_quality_chaos(
        ...     orders,
        ...     chaos_rate=0.05,
        ...     allowed_chaos_types=["duplicate", "fraud_fake_cpf", "incomplete_payment"],
        ...     verbose=True
        ... )
        >>> # Output: ✅ Chaos applied: 5 anomalies injected (3 duplicate, 1 fraud_fake_cpf, 1 incomplete_payment)
    """
    if not batch:
        return batch

    # Clamp chaos_rate
    chaos_rate = max(0.01, min(chaos_rate, 0.10))

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    # Filter available chaos types
    available_chaos_types = list(CHAOS_STRATEGIES.keys())
    chaos_types = allowed_chaos_types if allowed_chaos_types else available_chaos_types

    # Validate that all requested types exist
    invalid_types = set(chaos_types) - set(available_chaos_types)
    if invalid_types:
        print(f"⚠️ Invalid chaos types requested (ignored): {invalid_types}")
        chaos_types = [ct for ct in chaos_types if ct in available_chaos_types]

    if not chaos_types:
        print("⚠️ No valid chaos types available. Returning batch unchanged.")
        return batch

    chaos_applied = {}

    for idx in chaos_indices:
        chaos_type = random.choice(chaos_types)
        chaos_strategy = CHAOS_STRATEGIES[chaos_type]

        # Track applied chaos types
        chaos_applied[chaos_type] = chaos_applied.get(chaos_type, 0) + 1

        # Apply the chaos strategy
        try:
            result = chaos_strategy(batch[idx])

            # If the strategy returned a new record (duplication), append it to the batch
            if result is not None:
                batch.append(result)

        except Exception as e:
            print(f"⚠️ Error applying chaos type '{chaos_type}': {e}")

    if verbose and chaos_applied:
        total_anomalies = sum(chaos_applied.values())
        chaos_summary = ", ".join([f"{count} {ctype}" for ctype, count in chaos_applied.items()])
        print(f"✅ Chaos applied: {total_anomalies} anomalies injected ({chaos_summary})")

    return batch


def generate_chaos_metadata(
    table_name: str, chaos_rate: float, chaos_types: list[str]
) -> dict[str, Any]:
    """
    Generates chaos engineering metadata for documentation.

    Args:
        table_name: Table name (e.g. "sales.salesorder")
        chaos_rate: Chaos rate applied
        chaos_types: List of chaos types injected

    Returns:
        Dictionary with structured metadata

    Example:
        >>> metadata = generate_chaos_metadata(
        ...     "sales.salesorder",
        ...     0.05,
        ...     ["duplicate", "fraud_fake_cpf", "incomplete_payment"]
        ... )
    """
    return {
        "table": table_name,
        "chaos_engineering": {
            "enabled": True,
            "chaos_rate": chaos_rate,
            "chaos_types": chaos_types,
            "documentation": "See dbapi/api/seeder/CHAOS_DOCUMENTATION.md for details",
        },
    }


def get_available_chaos_types() -> list[str]:
    """Returns a list of all available chaos types."""
    return list(CHAOS_STRATEGIES.keys())
