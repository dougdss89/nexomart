"""
Chaos engineering para os seeders de purchasing (header, detail, vendor, audit).
Ver dbapi/api/seeder/CHAOS_DOCUMENTATION.md secao 3.9 para o catalogo completo.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Optional


def apply_purchaseorderheader_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Matematica financeira que nao bate, ship_date antes de order_date e pedido fantasma."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["financial_mismatch", "reversed_ship_date", "phantom_order"])

        if chaos_type == "financial_mismatch":
            # Desconto manual aplicado direto no total, sem recalcular sub_total/tax_amt
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
    """Over-shipment (recebido+rejeitado > pedido) e pedidos zumbis (pendentes ha anos)."""
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
    Fornecedor 'caloteiro ativo' e colisao de master data (mesmo tax_id, nomes diferentes).

    Espera batch ja validado/model_dump() pelo schema Vendor. O caso 'active_deadbeat'
    muta o dict serializado DEPOIS da validacao, contornando de proposito o model_validator
    que bloqueia credit_rating baixo + is_active=True (simula alguem forcando a ativacao
    fora da API, direto no banco).
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
    """Processo pulado (remove etapas intermediarias) e aprovador fantasma."""
    if not batch:
        return batch

    by_header: dict[str, list[int]] = {}
    for i, row in enumerate(batch):
        by_header.setdefault(row["purchaseorderheader_id"], []).append(i)

    header_ids = list(by_header.keys())
    num_chaos = max(1, int(len(header_ids) * chaos_rate))
    chosen_headers = random.sample(header_ids, min(num_chaos, len(header_ids)))

    remove_indices: set[int] = set()
    max_valid_id = max(valid_employee_ids) if valid_employee_ids else 0
    non_managers = [e for e in valid_employee_ids if not manager_employee_ids or e not in manager_employee_ids]

    for header_id in chosen_headers:
        indices = by_header[header_id]
        chaos_type = random.choice(["skipped_step", "ghost_approver"])

        if chaos_type == "skipped_step" and len(indices) >= 3:
            # Mantem só o primeiro evento (CREATED) e o ultimo, removendo as etapas do meio
            for i in indices[1:-1]:
                remove_indices.add(i)

        elif chaos_type == "ghost_approver":
            approval_indices = [i for i in indices if batch[i]["action"] in ("approved", "closed")]
            if not approval_indices:
                continue
            target = random.choice(approval_indices)
            if non_managers and random.random() < 0.5:
                batch[target]["employee_id"] = random.choice(non_managers)
            else:
                batch[target]["employee_id"] = max_valid_id + random.randint(100, 999)

    return [row for i, row in enumerate(batch) if i not in remove_indices]
