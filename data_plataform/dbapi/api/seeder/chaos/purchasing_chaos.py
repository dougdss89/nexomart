"""
Chaos engineering for the purchasing seeders (header, detail, vendor, audit).
See dbapi/api/seeder/CHAOS_DOCUMENTATION.md section 3.9 for the full catalog.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional


def apply_purchaseorderheader_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Financial math that doesn't add up, ship_date before order_date, and a phantom order."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["financial_mismatch", "reversed_ship_date", "phantom_order"])

        if chaos_type == "financial_mismatch":
            # Manual discount applied directly to the total, without recalculating sub_total/tax_amt
            discount = (row["sub_total"] * Decimal(str(random.uniform(0.05, 0.20)))).quantize(Decimal("0.0001"))
            row["total_due"] = row["total_due"] - discount

        elif chaos_type == "reversed_ship_date":
            row["ship_date"] = row["order_date"] - timedelta(days=random.randint(1, 15))

        elif chaos_type == "phantom_order":
            row["status"] = "completed"
            row["sub_total"] = Decimal("0.0000")
            row["tax_amt"] = Decimal("0.0000")
            row["freight"] = Decimal("0.0000")
            row["total_due"] = Decimal("0.0000")

    return batch


def apply_purchaseorderdetail_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Over-shipment (received+rejected > ordered) and zombie orders (pending for years)."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["overshipment", "stale_order"])

        if chaos_type == "overshipment":
            row["received_qty"] = row["order_qty"] + random.randint(1, 5)
            row["rejected_qty"] = random.randint(0, 2)
            row["stocked_qty"] = row["received_qty"] - row["rejected_qty"]
            row["is_fully_received"] = True

        elif chaos_type == "stale_order":
            row["is_fully_received"] = False
            row["received_qty"] = 0
            row["stocked_qty"] = 0
            row["due_date"] = datetime.now() - timedelta(days=random.randint(1000, 1200))

    return batch


def apply_vendor_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.03) -> list[dict[str, Any]]:
    """
    'Active deadbeat' vendor and master data collision (same tax_id, different names).

    Expects a batch already validated/model_dump()'d by the Vendor schema. The 'active_deadbeat'
    case mutates the serialized dict AFTER validation, deliberately bypassing the model_validator
    that blocks low credit_rating + is_active=True (simulates someone forcing activation
    outside the API, straight into the database).
    """
    if len(batch) < 2:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["active_deadbeat", "master_data_collision"])

        if chaos_type == "active_deadbeat":
            row["credit_rating"] = Decimal(random.choice([1, 2]))
            row["is_active"] = True

        elif chaos_type == "master_data_collision":
            other_idx = random.choice([i for i in range(len(batch)) if i != idx])
            row["vendor_details"]["tax_id"] = batch[other_idx]["vendor_details"]["tax_id"]

    return batch


def apply_purchase_audit_chaos(
    batch: list[dict[str, Any]],
    valid_employee_ids: list[int],
    manager_employee_ids: Optional[list[int]] = None,
    chaos_rate: float = 0.05,
) -> list[dict[str, Any]]:
    """Skipped process (removes intermediate steps) and ghost approver."""
    if not batch:
        return batch

    by_header: dict[str, list[int]] = {}
    for i, row in enumerate(batch):
        by_header.setdefault(row["purchaseorderheader_id"], []).append(i)

    header_ids = list(by_header.keys())
    num_chaos = max(1, int(len(header_ids) * chaos_rate))
    chosen_headers = random.sample(header_ids, min(num_chaos, len(header_ids)))

    remove_indices: set[int] = set()
    non_managers = [e for e in valid_employee_ids if not manager_employee_ids or e not in manager_employee_ids]

    for header_id in chosen_headers:
        indices = by_header[header_id]
        chaos_type = random.choice(["skipped_step", "ghost_approver"])

        if chaos_type == "skipped_step" and len(indices) >= 3:
            # Keeps only the first event (CREATED) and the last, removing the middle steps
            for i in indices[1:-1]:
                remove_indices.add(i)

        elif chaos_type == "ghost_approver":
            # employee_id has a NOT NULL FK to humanresources.employee — the "approver without
            # authorization" anomaly needs to keep pointing to a real employee
            # (never a made-up id, which would break the constraint), so the fraud
            # signal is flagged in the obs text instead of a phantom employee_id.
            approval_indices = [i for i in indices if batch[i]["action"] in ("approved", "closed")]
            if not approval_indices:
                continue
            target = random.choice(approval_indices)
            batch[target]["employee_id"] = random.choice(non_managers or valid_employee_ids)
            batch[target]["obs"] = "[ALERTA] Aprovador sem autorização no sistema"

    return [row for i, row in enumerate(batch) if i not in remove_indices]
