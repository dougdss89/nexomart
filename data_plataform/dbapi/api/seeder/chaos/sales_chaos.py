"""
Chaos engineering para os seeders de sales (order, eventlog, return_order, seller).
Ver dbapi/api/seeder/CHAOS_DOCUMENTATION.md secao 3.10 para o catalogo completo.
"""

import random
from datetime import timedelta
from decimal import Decimal
from typing import Any


def apply_sales_order_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Matematica quebrada (desconto fantasma), pagamento esquizofrenico e pedido de Schrodinger."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["broken_math", "schizophrenic_payment", "schrodinger_order"])

        if chaos_type == "broken_math":
            # Desconto aplicado no total sem ratear entre as linhas do pedido
            discount = (row["total_amount"] * Decimal(str(random.uniform(0.3, 0.7)))).quantize(Decimal("0.01"))
            row["total_amount"] = row["total_amount"] - discount

        elif chaos_type == "schizophrenic_payment":
            total = row["total_amount"]
            thirds = (total / 3).quantize(Decimal("0.01"))
            row["payment_info"] = {
                "is_split": False,
                "transactions": [
                    {"method": "pix", "amount": float(thirds), "status": "paid"},
                    {"method": "credit_card", "amount": float(thirds), "status": "paid"},
                    {"method": "credit_card", "amount": float(total - thirds * 2), "status": "paid"},
                ],
            }

        elif chaos_type == "schrodinger_order":
            row["order_status"] = "finalized"
            payment_info = row.get("payment_info") or {"is_split": False, "transactions": [{}]}
            if payment_info.get("transactions"):
                payment_info["transactions"][0]["status"] = random.choice(["unpaid", "refunded"])
            row["payment_info"] = payment_info

    return batch


def apply_eventlog_chaos(events: list[Any], chaos_rate: float = 0.05) -> list[Any]:
    """
    Viagem no tempo logistica (evento tardio chega com data anterior a um evento
    anterior do mesmo pedido) e teletransporte GPS (Null Island ou coordenada impossivel).

    Opera sobre instancias ja construidas de SalesOrderEventLogSchema, mutando atributos
    depois da validacao (Pydantic nao revalida em atribuicao simples sem
    validate_assignment=True).
    """
    if not events:
        return events

    by_order: dict[int, list[int]] = {}
    for i, event in enumerate(events):
        by_order.setdefault(event.order_id, []).append(i)

    orders_with_multiple_events = [oid for oid, idxs in by_order.items() if len(idxs) >= 2]
    if not orders_with_multiple_events:
        return events

    num_chaos = max(1, int(len(orders_with_multiple_events) * chaos_rate))
    chosen_orders = random.sample(orders_with_multiple_events, min(num_chaos, len(orders_with_multiple_events)))

    for order_id in chosen_orders:
        indices = by_order[order_id]
        chaos_type = random.choice(["time_travel", "gps_teleport"])

        if chaos_type == "time_travel" and len(indices) >= 2:
            earlier_idx, later_idx = sorted(random.sample(indices, 2))
            # O evento posterior (na sequencia logica) chega com timestamp anterior ao evento anterior
            events[later_idx].event_date = events[earlier_idx].event_date - timedelta(hours=random.randint(1, 4))

        elif chaos_type == "gps_teleport":
            target_idx = random.choice(indices)
            gps = events[target_idx].event_data.gps_location
            if gps is not None:
                if random.random() < 0.5:
                    gps.lat, gps.long = 0.0, 0.0  # Null Island
                else:
                    gps.lat, gps.long = 999.9, 999.9  # fisicamente impossivel

    return events


def apply_return_order_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Reembolso infinito (10x o valor original) e inspecao fantasma (estorno sem inspecao real)."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["infinite_refund", "phantom_inspection"])

        if chaos_type == "infinite_refund":
            row["refund_amount"] = (row["refund_amount"] * Decimal("10")).quantize(Decimal("0.0001"))

        elif chaos_type == "phantom_inspection":
            row["return_status"] = "refunded"
            inspection = row.get("inspection_data") or {}
            inspection["is_restockable"] = True
            inspection["inspector_user_id"] = None
            inspection["technical_notes"] = None
            row["inspection_data"] = inspection

    return batch


def apply_seller_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Parceiro toxico (chargeback alto + rating baixo, mas ainda ativo) e dados bancarios sujos."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["toxic_partner", "dirty_bank_details"])
        demographics = row.get("demographics") or {}

        if chaos_type == "toxic_partner":
            metrics = demographics.get("metrics") or {}
            metrics["chargeback_rate"] = round(random.uniform(0.5, 0.9), 4)
            metrics["average_rating"] = round(random.uniform(1.0, 1.5), 1)
            demographics["metrics"] = metrics
            row["is_active"] = True

        elif chaos_type == "dirty_bank_details":
            bank_account = demographics.get("bank_account") or {}
            if random.random() < 0.5:
                bank_account["account_number"] = f"{random.randint(10000, 99999)}-X"
            else:
                bank_account["agency"] = ""
            demographics["bank_account"] = bank_account

        row["demographics"] = demographics

    return batch
