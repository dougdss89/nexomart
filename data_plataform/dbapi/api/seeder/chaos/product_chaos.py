"""
Chaos engineering para os seeders de product (catalogo, categoria e embeddings).
Ver dbapi/api/seeder/CHAOS_DOCUMENTATION.md secao 1 (Anomalias de Qualidade de Dados)
para o catalogo completo. As anomalias ja existentes em api/samples/product.py
(peso negativo, imposto zerado, brand em maiusculo, dimensao faltando, schema
evolution) nao sao duplicadas aqui.
"""

import math
import random
from datetime import datetime, timedelta
from typing import Any


def apply_product_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Margem negativa, peso sujo (string), nome quase-duplicado e datas de venda ilogicas."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        details = row.get("productdetails") or {}
        chaos_type = random.choice([
            "negative_margin", "dirty_weight_unit", "duplicate_inconsistent_name", "illogical_sell_dates",
        ])

        if chaos_type == "negative_margin":
            # price (venda) fica abaixo de list_price (custo) ou zerado - "glitch" de promocao
            if random.random() < 0.5:
                details["price"] = 0.0
            else:
                details["price"] = round(details.get("list_price", 1.0) * random.uniform(0.3, 0.9), 2)

        elif chaos_type == "dirty_weight_unit":
            weight = details.get("weight")
            if weight is not None:
                details["weight"] = random.choice([
                    f"{weight}kg", f"{str(weight).replace('.', ',')} kg", f"{weight} KG",
                ])

        elif chaos_type == "duplicate_inconsistent_name":
            other_idx = random.choice([i for i in range(len(batch)) if i != idx])
            base_name = batch[other_idx]["product_name"]
            row["product_name"] = base_name.replace(" ", "-")

        elif chaos_type == "illogical_sell_dates":
            start = row.get("sell_start_date", datetime.now())
            row["sell_end_date"] = start - timedelta(days=random.randint(1, 90))

        row["productdetails"] = details

    return batch


def apply_category_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Categoria orfa (parent_category_id apontando pra id inexistente) e ciclo entre duas categorias."""
    if len(batch) < 2:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))
    max_id = max(row["category_id"] for row in batch)

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["orphan_parent", "cyclic_parent"])

        if chaos_type == "orphan_parent":
            row["parent_category_id"] = max_id + random.randint(100, 999)

        elif chaos_type == "cyclic_parent":
            other_idx = random.choice([i for i in range(len(batch)) if i != idx])
            batch[idx]["parent_category_id"] = batch[other_idx]["category_id"]
            batch[other_idx]["parent_category_id"] = batch[idx]["category_id"]

    return batch


def apply_embedding_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Falha de extracao de imagem, vetor truncado (dimension mismatch) e valores NaN/Infinitos."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(["failed_image_extraction", "truncated_vector", "nan_infinite_values"])

        if chaos_type == "failed_image_extraction":
            row["image_vector"] = None
            row["ai_metadata"]["image_model"] = "failed_extraction"

        elif chaos_type == "truncated_vector" and row.get("description_vector"):
            row["description_vector"] = row["description_vector"][:-1]

        elif chaos_type == "nan_infinite_values" and row.get("description_vector"):
            vector = row["description_vector"]
            for _ in range(random.randint(1, 5)):
                vector[random.randrange(len(vector))] = random.choice([math.nan, math.inf, -math.inf])

    return batch
