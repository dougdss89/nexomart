import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta, date
from decimal import Decimal
from sqlalchemy import select
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_daily_performance import MarketingDailyPerformance, MarketingMetrics
from api.models.marketing.marketing_advertise import MarketingAdvertiseModel
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_channel import MarketingChannelModel
from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel
from api.seeder.chaos import apply_daily_performance_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args, bulk_copy

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Widened 2026-09-05 so history spans back to 2020 instead of just 7-30 days.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1).date()

# This function has no watermark of its own — every run regenerates a fresh history window
# per ad with no memory of what a previous run already inserted, so volume has been growing
# unbounded (349M rows measured 2026-09-05). Real fix (giving this a watermark like the fan-out
# seeders have) is tracked as a follow-up; this cap only bounds the damage per run in the
# meantime, it does not fix the underlying duplication.
MAX_ROWS_PER_RUN = 5_000


def generate_performance_payloads(
    real_ads: list[dict],
    platform_by_campaign: dict[int, str] | None = None,
    budget_by_campaign: dict[int, Decimal] | None = None,
    chaos_rate: float = 0.05,
    apply_chaos: bool = True,
) -> list[dict]:
    """
    Generates the Daily Performance Fact Table with a mathematical funnel.
    """
    batch = []
    today = datetime.now().date()
    performance_id_counter = 1

    for ad in real_ads:
        if len(batch) >= MAX_ROWS_PER_RUN:
            break

        # Generates 7 days up to the full 2020-today range of history
        history_days = random.randint(7, (today - EARLIEST_TRANSACTION_DATE).days)
        start_date = today - timedelta(days=history_days)

        is_gsn = "GSN" in ad["ad_name"].upper()
        media_type = ad["creative_data"].get("media_type", "image")

        for j in range(history_days + 1):
            if len(batch) >= MAX_ROWS_PER_RUN:
                break
            ref_date = start_date + timedelta(days=j)

            # --- Mathematical Funnel ---
            impressions = random.randint(100, 5000)
            ctr = random.uniform(0.015, 0.05) if is_gsn else random.uniform(0.008, 0.03)
            clicks = int(impressions * ctr)
            cpc = Decimal(random.uniform(0.40, 3.50)).quantize(Decimal("0.0001"))
            cost = (Decimal(clicks) * cpc).quantize(Decimal("0.0001"))
            
            cvr = random.uniform(0.01, 0.04)
            conversions = int(clicks * cvr)
            aov = Decimal(random.uniform(80.0, 450.0)).quantize(Decimal("0.01"))
            conversion_value = (Decimal(conversions) * aov).quantize(Decimal("0.01"))
            
            metrics_payload = {
                "reach": int(impressions * 0.8),
                "frequency": 1.25,
                "adds_to_cart": conversions + random.randint(1, 5),
                "checkouts_initiated": conversions + random.randint(0, 2)
            }
            
            if is_gsn:
                metrics_payload["quality_score"] = random.randint(6, 10)
                metrics_payload["search_impression_share"] = round(random.uniform(0.5, 0.9), 4)

            payload = {
                "performance_id": performance_id_counter,  # placeholder for Pydantic validation (gt=0); removed before insert
                "campaign_id": ad["campaign_id"],
                "ad_group_id": ad["ad_group_id"],
                "ad_id": ad["ad_id"],
                "date_ref": ref_date,
                "cost": cost,
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "conversion_value": conversion_value,
                "metrics": metrics_payload,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
            performance_obj = MarketingDailyPerformance(**payload)
            validated_performance = performance_obj.model_dump()
            # metrics is JSONB and has an internal datetime (created_at) — needs to become a JSON-safe type
            validated_performance["metrics"] = performance_obj.metrics.model_dump(mode="json")
            validated_performance.pop("performance_id", None)  # let the database's autoincrement take over
            batch.append(validated_performance)
            performance_id_counter += 1

    if apply_chaos:
        batch = apply_daily_performance_chaos(
            batch,
            chaos_rate=chaos_rate,
            platform_by_campaign=platform_by_campaign,
            budget_by_campaign=budget_by_campaign,
        )

    return batch

async def seed_marketing_performance(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """
    Runs the performance load consuming the REAL hierarchy from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch the ads -> adgroups -> campaigns mesh
        query = (
            select(
                MarketingAdvertiseModel.ad_id,
                MarketingAdvertiseModel.ad_name,
                MarketingAdvertiseModel.creative_data,
                MarketingAdvertiseModel.ad_group_id,
                MarketingAdGroupModel.campaign_id
            )
            .join(MarketingAdGroupModel, MarketingAdvertiseModel.ad_group_id == MarketingAdGroupModel.ad_group_id)
        )
        result = await session.execute(query)
        real_ads = [
            {
                "ad_id": row[0],
                "ad_name": row[1],
                "creative_data": row[2],
                "ad_group_id": row[3],
                "campaign_id": row[4]
            }
            for row in result.all()
        ]

        if not real_ads:
            print("⚠️ No Ad found! Run the previous marketing seeders first.")
            return

        # 2. Fetch platform and budget per campaign (needed for the cross-platform
        # field leak and budget overspend chaos)
        campaign_query = (
            select(
                MarketingCampaignModel.campaign_id,
                MarketingCampaignModel.total_budget,
                MarketingChannelModel.platform_name,
            )
            .join(MarketingChannelModel, MarketingCampaignModel.channel_id == MarketingChannelModel.channel_id)
        )
        campaign_result = await session.execute(campaign_query)
        budget_by_campaign = {}
        platform_by_campaign = {}
        for row in campaign_result.all():
            budget_by_campaign[row[0]] = row[1]
            platform_by_campaign[row[0]] = row[2]

        print(f"📣 Seeding daily performance for {len(real_ads)} ads | chaos={chaos_rate:.2%}")
        performances = generate_performance_payloads(
            real_ads,
            platform_by_campaign=platform_by_campaign,
            budget_by_campaign=budget_by_campaign,
            chaos_rate=chaos_rate,
            apply_chaos=apply_chaos,
        )

        # Insert in batches of 1000 to avoid timeouts or buffer overflow
        batch_size = 1000
        for i in range(0, len(performances), batch_size):
            current_batch = performances[i:i+batch_size]
            try:
                await bulk_copy(session, MarketingDailyPerformanceModel, current_batch)
                print(f"✅ Inserting batch {i//batch_size + 1} ({len(current_batch)} rows)...")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error in batch: {e}")
                raise RuntimeError(f"Marketing daily performance seeding aborted at batch {i//batch_size + 1}: {e}") from e

        print(f"🚀 Performance seeder finished successfully!")

if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_dailyperformance (fan-out of real ads)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_marketing_performance(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
