"""
Chaos engineering para os seeders de warehouse (transaction_history, product_inventory,
warehouse_location). Ver dbapi/api/seeder/CHAOS_DOCUMENTATION.md secao 3.11 para o
catalogo completo.
"""

import random
from datetime import date, timedelta
from typing import Any, Optional


def apply_transaction_history_chaos(
    batch: list[Any],
    cold_storage_location_ids: set[int],
    neutral_type_ids: set[int],
    chaos_rate: float = 0.05,
) -> list[Any]:
    """
    Quebra de cadeia de frio (temp_at_scan absurda numa transacao ligada a location
    cold_storage) e buraco negro logistico (transferencia com target perdido).

    Opera sobre instancias ja construidas de TransactionHistorySchema, mutando atributos
    depois da validacao (mesmo padrao usado em sales_chaos.apply_eventlog_chaos).
    """
    if not batch:
        return batch

    cold_chain_candidates = [
        i for i, txn in enumerate(batch)
        if txn.traceability_info.source_warehouselocation_id in cold_storage_location_ids
        or txn.traceability_info.target_warehouselocation_id in cold_storage_location_ids
    ]
    black_hole_candidates = [
        i for i, txn in enumerate(batch)
        if txn.transactiontype_id in neutral_type_ids
        and txn.traceability_info.target_warehouselocation_id is not None
    ]

    def sample_chaos(candidates: list[int]) -> list[int]:
        if not candidates:
            return []
        num_chaos = max(1, int(len(candidates) * chaos_rate))
        return random.sample(candidates, min(num_chaos, len(candidates)))

    for idx in sample_chaos(cold_chain_candidates):
        iot = batch[idx].traceability_info.iot_sensor_data
        if iot is not None:
            iot.temp_at_scan = 45.0

    for idx in sample_chaos(black_hole_candidates):
        batch[idx].traceability_info.target_warehouselocation_id = None

    return batch


def apply_inventory_chaos(batch: list[Any], chaos_rate: float = 0.05) -> list[Any]:
    """
    Zumbi perecivel (estoque alto e ativo, mas vencido ha anos) e lote quantico
    (produto em estoque sem lot_number/manufacturing_date, quebrando rastreabilidade).

    Opera sobre instancias ja construidas de ProductInventorySchema.
    """
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        item = batch[idx]
        chaos_type = random.choice(["perishable_zombie", "quantum_lot"])

        if chaos_type == "perishable_zombie":
            item.quantity = random.randint(3000, 8000)
            item.is_active = True
            expiration = date.today() - timedelta(days=random.randint(700, 900))
            # Mantem o validator (expiration >= manufacturing) satisfeito
            item.attributes.expiration_date = expiration
            item.attributes.manufacturing_date = expiration - timedelta(days=random.randint(200, 400))

        elif chaos_type == "quantum_lot":
            item.attributes.lot_number = None
            item.attributes.manufacturing_date = None

    return batch


def apply_warehouse_location_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Cadastro sujo (ja existente), incompatibilidade de temperatura e teletransporte geografico."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["dirty_registration", "temperature_mismatch", "geo_teleport"])

        if chaos_type == "dirty_registration":
            row["geo_location"]["postal_code"] = "INVALID_ZIP"
            row["name"] = None

        elif chaos_type == "temperature_mismatch":
            row["capabilities"]["warehouse_type"] = "cold_storage"
            row["capabilities"]["temperature_range"] = {"min_temp": 25.0, "max_temp": 40.0}

        elif chaos_type == "geo_teleport":
            row["geo_location"]["postal_code"] = random.choice(["90210", "10001-1234", "60601"])

    return batch
