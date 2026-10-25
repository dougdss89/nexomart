import asyncio
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from api.database import AsyncSessionLocal
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.models.marketing.marketing_advertise import MarketingAdvertiseModel
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_channel import MarketingChannelModel
from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel
from api.schemas.marketing.marketing_daily_performance import (
    MarketingDailyPerformance,
)
from api.seeder.chaos import apply_daily_performance_chaos
from api.utils.cli_common import add_chaos_args, build_parser, bulk_copy, resolve_chaos_args
from sqlalchemy import select, text

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

# Widened 2026-09-05 so history spans back to 2020 instead of just 7-30 days.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1).date()

# Historically this function had no watermark of its own — every run regenerated a fresh
# history window per ad with no memory of what a previous run already inserted, so volume grew
# unbounded (349M rows measured 2026-09-05, most of it duplicate ad+date_ref facts). Fixed
# 2026-09-26 by giving each ad a per-ad watermark (see `_fetch_last_date_by_ad`/
# `_update_ad_watermarks`/`generate_performance_payloads`'s `last_date_by_ad` param): a run now
# only fills in days between an ad's last generated `date_ref` and today, so an ad that's
# already caught up contributes 0 rows until the next calendar day. MAX_ROWS_PER_RUN stays as a
# belt-and-braces cap — still needed for the first run against a large ad pool (each
# previously-unseeded ad still backfills a random slice of history) and for the day a lot of ads
# all roll over to a new day at once.
MAX_ROWS_PER_RUN = 5_000

# 2026-09-26's first attempt at the watermark above queried
# `dailyperformance.ad_id IN (<every real ad id>)` directly — broke every run the next day
# (2026-09-27) once `marketing.advertise` passed ~33k rows: asyncpg hard-caps a single query at
# 32,767 bound parameters, and that `IN` list is one parameter per ad. Even an unfiltered
# `GROUP BY ad_id` over the whole table wouldn't have been a real fix either — dailyperformance
# has no index on `ad_id` at all, so it'd be a full scan of a table already at hundreds of
# millions of rows, every 15 minutes, forever. Fixed by keeping the per-ad watermark in its own
# tiny side table instead (same pattern as ingestion_common.py's `ingestion_control.watermarks`
# for the landing-zone ingestion, applied here to the seeder itself): O(number of ads), not
# O(table size), and no parameter list to overflow.
AD_WATERMARK_SCHEMA = "marketing_control"
AD_WATERMARK_TABLE = "ad_performance_watermarks"


async def _ensure_ad_watermark_table(session) -> None:
    """Self-bootstraps the control table — no separate migration step to remember, mirrors
    ingestion_common.py's `_ensure_watermark_table`."""
    await session.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{AD_WATERMARK_SCHEMA}"'))
    await session.execute(
        text(f'''
        CREATE TABLE IF NOT EXISTS "{AD_WATERMARK_SCHEMA}"."{AD_WATERMARK_TABLE}" (
            ad_id integer PRIMARY KEY,
            last_date_ref date NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now()
        )
    ''')
    )


async def _fetch_last_date_by_ad(session) -> dict[int, date]:
    """Per-ad watermark: the latest `date_ref` this ad already has a row for, so a run only
    fills in the gap between that and today instead of regenerating the ad's whole history from
    scratch every time. Reads the small side table (see `AD_WATERMARK_SCHEMA`/`_TABLE` above),
    never `marketing.dailyperformance` itself — keeps this O(number of ads) regardless of how
    big the fact table grows."""
    await _ensure_ad_watermark_table(session)
    rows = await session.execute(
        text(f'SELECT ad_id, last_date_ref FROM "{AD_WATERMARK_SCHEMA}"."{AD_WATERMARK_TABLE}"')
    )
    return dict(rows.all())


async def _update_ad_watermarks(session, last_date_by_ad: dict[int, date]) -> None:
    """UPSERTs this run's newest `date_ref` per ad into the watermark side table — called after
    the batch has been successfully inserted, so a failed/rolled-back run never advances a
    watermark past rows that don't actually exist yet."""
    if not last_date_by_ad:
        return
    await session.execute(
        text(f'''
            INSERT INTO "{AD_WATERMARK_SCHEMA}"."{AD_WATERMARK_TABLE}" (ad_id, last_date_ref)
            VALUES (:ad_id, :last_date_ref)
            ON CONFLICT (ad_id) DO UPDATE SET
                last_date_ref = GREATEST(EXCLUDED.last_date_ref, "{AD_WATERMARK_TABLE}".last_date_ref),
                updated_at = now()
        '''),
        [
            {"ad_id": ad_id, "last_date_ref": last_date}
            for ad_id, last_date in last_date_by_ad.items()
        ],
    )
    await session.commit()


def generate_performance_payloads(
    real_ads: list[dict],
    last_date_by_ad: dict[int, date] | None = None,
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
    last_date_by_ad = last_date_by_ad or {}

    for ad in real_ads:
        if len(batch) >= MAX_ROWS_PER_RUN:
            break

        last_date = last_date_by_ad.get(ad["ad_id"])
        if last_date is not None:
            # Already has history: only catch up the gap since the last row generated for it —
            # 0 new rows for an ad that's already current as of today, instead of regenerating
            # a fresh random slice of its whole history every single run.
            start_date = last_date + timedelta(days=1)
            history_days = (today - start_date).days
            if history_days < 0:
                continue
        else:
            # First time seeding this ad: backfill a random slice of history, same as before.
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
                "checkouts_initiated": conversions + random.randint(0, 2),
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
                "row_guid": uuid.uuid4(),
            }
            performance_obj = MarketingDailyPerformance(**payload)
            validated_performance = performance_obj.model_dump()
            # metrics is JSONB and has an internal datetime (created_at) — needs to become a JSON-safe type
            validated_performance["metrics"] = performance_obj.metrics.model_dump(mode="json")
            validated_performance.pop(
                "performance_id", None
            )  # let the database's autoincrement take over
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
        query = select(
            MarketingAdvertiseModel.ad_id,
            MarketingAdvertiseModel.ad_name,
            MarketingAdvertiseModel.creative_data,
            MarketingAdvertiseModel.ad_group_id,
            MarketingAdGroupModel.campaign_id,
        ).join(
            MarketingAdGroupModel,
            MarketingAdvertiseModel.ad_group_id == MarketingAdGroupModel.ad_group_id,
        )
        result = await session.execute(query)
        real_ads = [
            {
                "ad_id": row[0],
                "ad_name": row[1],
                "creative_data": row[2],
                "ad_group_id": row[3],
                "campaign_id": row[4],
            }
            for row in result.all()
        ]

        if not real_ads:
            print("⚠️ No Ad found! Run the previous marketing seeders first.")
            return

        # 1b. Per-ad watermark (see _fetch_last_date_by_ad) — this is what actually stops the
        # unbounded duplication MAX_ROWS_PER_RUN's docstring describes.
        last_date_by_ad = await _fetch_last_date_by_ad(session)

        # 2. Fetch platform and budget per campaign (needed for the cross-platform
        # field leak and budget overspend chaos)
        campaign_query = select(
            MarketingCampaignModel.campaign_id,
            MarketingCampaignModel.total_budget,
            MarketingChannelModel.platform_name,
        ).join(
            MarketingChannelModel,
            MarketingCampaignModel.channel_id == MarketingChannelModel.channel_id,
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
            last_date_by_ad=last_date_by_ad,
            platform_by_campaign=platform_by_campaign,
            budget_by_campaign=budget_by_campaign,
            chaos_rate=chaos_rate,
            apply_chaos=apply_chaos,
        )

        # Insert in batches of 1000 to avoid timeouts or buffer overflow
        batch_size = 1000
        for i in range(0, len(performances), batch_size):
            current_batch = performances[i : i + batch_size]
            try:
                await bulk_copy(session, MarketingDailyPerformanceModel, current_batch)
                print(f"✅ Inserting batch {i // batch_size + 1} ({len(current_batch)} rows)...")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error in batch: {e}")
                raise RuntimeError(
                    f"Marketing daily performance seeding aborted at batch {i // batch_size + 1}: {e}"
                ) from e

        # Advance each ad's watermark to the newest date_ref actually generated for it this run
        # — only after every batch above has committed, so a run that fails partway through
        # never advances a watermark past rows that don't exist in the table yet.
        new_last_date_by_ad: dict[int, date] = {}
        for row in performances:
            ad_id, ref_date = row["ad_id"], row["date_ref"]
            if ad_id not in new_last_date_by_ad or ref_date > new_last_date_by_ad[ad_id]:
                new_last_date_by_ad[ad_id] = ref_date
        await _update_ad_watermarks(session, new_last_date_by_ad)

        print("🚀 Performance seeder finished successfully!")


if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_dailyperformance (fan-out of real ads)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_marketing_performance(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
