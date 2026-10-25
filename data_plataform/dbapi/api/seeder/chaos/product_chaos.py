"""
Chaos engineering for the product seeders (catalog, category, and embeddings).
See dbapi/api/seeder/CHAOS_DOCUMENTATION.md section 1 (Data Quality Anomalies)
for the full catalog. The anomalies already present in api/samples/product.py
(negative weight, zeroed tax, uppercase brand, missing dimension, schema
evolution) are not duplicated here.
"""

import random
from datetime import datetime, timedelta
from typing import Any


def apply_product_chaos(
    batch: list[dict[str, Any]], chaos_rate: float = 0.05
) -> list[dict[str, Any]]:
    """Negative margin, dirty weight (string), near-duplicate name, and illogical sale dates."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        details = row.get("productdetails") or {}
        chaos_type = random.choice(
            [
                "negative_margin",
                "dirty_weight_unit",
                "duplicate_inconsistent_name",
                "illogical_sell_dates",
            ]
        )

        if chaos_type == "negative_margin":
            # price (sale) ends up below list_price (cost) or zeroed out - promotion "glitch"
            if random.random() < 0.5:
                details["price"] = 0.0
            else:
                details["price"] = round(
                    details.get("list_price", 1.0) * random.uniform(0.3, 0.9), 2
                )

        elif chaos_type == "dirty_weight_unit":
            weight = details.get("weight")
            if weight is not None:
                details["weight"] = random.choice(
                    [
                        f"{weight}kg",
                        f"{str(weight).replace('.', ',')} kg",
                        f"{weight} KG",
                    ]
                )

        elif chaos_type == "duplicate_inconsistent_name":
            other_idx = random.choice([i for i in range(len(batch)) if i != idx])
            base_name = batch[other_idx]["product_name"]
            row["product_name"] = base_name.replace(" ", "-")

        elif chaos_type == "illogical_sell_dates":
            start = row.get("sell_start_date", datetime.now())
            row["sell_end_date"] = start - timedelta(days=random.randint(1, 90))

        row["productdetails"] = details

    return batch


def apply_category_chaos(
    batch: list[dict[str, Any]], chaos_rate: float = 0.05
) -> list[dict[str, Any]]:
    """Orphan category (parent_category_id pointing to a nonexistent id) and a cycle between two categories."""
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


def apply_embedding_chaos(
    batch: list[dict[str, Any]], chaos_rate: float = 0.05
) -> list[dict[str, Any]]:
    """Image extraction failure, truncated vector (dimension mismatch), and NaN/Infinite values."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(
            ["failed_image_extraction", "truncated_vector", "nan_infinite_values"]
        )

        if chaos_type == "failed_image_extraction":
            row["image_vector"] = None
            row["ai_metadata"]["image_model"] = "failed_extraction"

        elif chaos_type == "truncated_vector" and row.get("description_vector"):
            # pgvector requires the exact dimension (Vector(1536)) — a truncated vector is
            # never actually insertable, so the extraction failure is marked as a null
            # vector + metadata (same pattern as failed_image_extraction), not as an
            # array of the wrong size.
            row["description_vector"] = None
            row["ai_metadata"]["text_model"] = "truncated_vector_error"

        elif chaos_type == "nan_infinite_values" and row.get("description_vector"):
            # pgvector rejects NaN/Infinity at the database level ("infinite value not allowed
            # in vector") — same handling as the extraction failure in truncated_vector.
            row["description_vector"] = None
            row["ai_metadata"]["text_model"] = "nan_infinite_extraction_error"

    return batch
