"""
Chaos engineering for the marketing seeders.
See dbapi/api/seeder/CHAOS_DOCUMENTATION.md section 3.6 for the full catalog
(pattern, detection, and expected correction) of each anomaly.

The marketing seeders validate the payload via Pydantic (`Model(**payload).model_dump()`)
before assembling the batch. That's why these functions operate on the already-serialized
`dict` (post model_dump), never on the raw payload before validation — this way they bypass
the "Pydantic Guard" without needing to loosen any schema.
"""

import random
import uuid
from decimal import Decimal
from typing import Any, Optional


def apply_campaign_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """3.6.1 broken naming, 3.6.2 zeroed budget with active status, 3.6.14 duplicate UTM."""
    if len(batch) < 2:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        chaos_type = random.choice(["broken_naming", "zero_budget_active", "duplicate_utm"])

        if chaos_type == "broken_naming":
            batch[idx]["campaign_name"] = random.choice([
                "Cópia de campanha teste 1", "nova campanha (1)", "teste 123", "SEM NOME",
            ])

        elif chaos_type == "zero_budget_active":
            batch[idx]["total_budget"] = Decimal("0.00")
            batch[idx]["status"] = "active"
            batch[idx]["is_active"] = True

        elif chaos_type == "duplicate_utm":
            other_idx = random.choice([i for i in range(len(batch)) if i != idx])
            batch[idx]["utm_campaign"] = batch[other_idx]["utm_campaign"]

    return batch


def apply_adgroup_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """3.6.4 missing/empty targeting, 3.6.5 inverted age range."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        chaos_type = random.choice(["empty_targeting", "inverted_age_range"])

        if chaos_type == "empty_targeting":
            batch[idx]["targeting_criteria"] = {}

        elif chaos_type == "inverted_age_range":
            targeting = dict(batch[idx].get("targeting_criteria") or {})
            targeting["age_range"] = {"min_age": 65, "max_age": 18}
            batch[idx]["targeting_criteria"] = targeting

    return batch


def apply_ads_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """3.6.6 broken links, 3.6.7 media_type incompatible with the asset extension."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        chaos_type = random.choice(["broken_url", "media_type_mismatch"])
        creative = dict(batch[idx].get("creative_data") or {})

        if chaos_type == "broken_url":
            creative["final_url"] = random.choice([
                "www.example.com/lp",
                "https://cdn.example.com/ad final.jpg",
                "cdn.example.com/sem-protocolo",
            ])

        elif chaos_type == "media_type_mismatch":
            creative["media_type"] = "video"
            creative["media_assets"] = [
                {"url": f"https://cdn.example.com/ad_{uuid.uuid4().hex[:6]}.jpg"}
            ]

        batch[idx]["creative_data"] = creative

    return batch


def apply_daily_performance_chaos(
    batch: list[dict[str, Any]],
    chaos_rate: float = 0.05,
    platform_by_campaign: Optional[dict[int, str]] = None,
    budget_by_campaign: Optional[dict[int, Decimal]] = None,
) -> list[dict[str, Any]]:
    """
    3.6.8 cross-platform field leak (requires platform_by_campaign),
    3.6.9 clicks > impressions, 3.6.10 zero cost with conversion,
    3.6.11 retroactive metrics (duplicates the row with revised values),
    3.6.12 non-monotonic video funnel, 3.6.13 reach > impressions,
    3.6.3 budget overspend (requires budget_by_campaign).
    """
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    chaos_types = [
        "clicks_exceed_impressions",
        "zero_cost_with_conversion",
        "retroactive_metric_revision",
        "non_monotonic_video_funnel",
        "reach_exceeds_impressions",
    ]
    if platform_by_campaign:
        chaos_types.append("cross_platform_field_leak")

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice(chaos_types)
        metrics = dict(row.get("metrics") or {})

        if chaos_type == "clicks_exceed_impressions":
            row["clicks"] = int(row["impressions"] * random.uniform(1.05, 1.3))

        elif chaos_type == "zero_cost_with_conversion":
            row["cost"] = Decimal("0.00")
            row["conversions"] = max(row.get("conversions", 0), random.randint(1, 5))

        elif chaos_type == "retroactive_metric_revision":
            revision = dict(row)
            revision["performance_id"] = None
            revision["conversions"] = row.get("conversions", 0) + random.randint(1, 10)
            revision["conversion_value"] = (
                Decimal(str(row.get("conversion_value", 0))) * Decimal("1.3")
            ).quantize(Decimal("0.01"))
            revision["row_guid"] = uuid.uuid4()
            batch.append(revision)

        elif chaos_type == "non_monotonic_video_funnel":
            metrics["video_views_25"] = 800
            metrics["video_views_50"] = 600
            metrics["video_views_75"] = 750  # violates: 75% > 50%
            metrics["video_views_100"] = 100

        elif chaos_type == "reach_exceeds_impressions":
            metrics["reach"] = int(row["impressions"] * random.uniform(1.1, 1.5))

        elif chaos_type == "cross_platform_field_leak":
            platform = (platform_by_campaign or {}).get(row.get("campaign_id"))
            if platform and platform != "google_ads":
                metrics["quality_score"] = random.randint(1, 10)

        row["metrics"] = metrics

    if budget_by_campaign:
        _apply_budget_overspend_chaos(batch, budget_by_campaign, chaos_rate)

    return batch


def _apply_budget_overspend_chaos(
    batch: list[dict[str, Any]],
    budget_by_campaign: dict[int, Decimal],
    chaos_rate: float,
) -> None:
    """3.6.3: inflates the daily cost of randomly chosen campaigns until the budget is blown by ~300%."""
    campaign_ids = list({
        row["campaign_id"] for row in batch if row.get("campaign_id") in budget_by_campaign
    })
    if not campaign_ids:
        return

    num_chaos = max(1, int(len(campaign_ids) * chaos_rate))
    chosen_campaigns = random.sample(campaign_ids, min(num_chaos, len(campaign_ids)))

    for campaign_id in chosen_campaigns:
        rows = [r for r in batch if r["campaign_id"] == campaign_id]
        if not rows:
            continue

        current_spend = sum(Decimal(str(r["cost"])) for r in rows)
        target_spend = budget_by_campaign[campaign_id] * Decimal("3.0")
        if current_spend >= target_spend:
            continue

        missing = target_spend - current_spend
        per_row_bump = (missing / len(rows)).quantize(Decimal("0.0001"))
        for r in rows:
            r["cost"] = (Decimal(str(r["cost"])) + per_row_bump).quantize(Decimal("0.0001"))
