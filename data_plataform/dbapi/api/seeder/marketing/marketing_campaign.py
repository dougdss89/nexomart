import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_campaign import MarketingCampaign, MarketingBudgeType, CampaignStatus
from api.models.marketing.marketing_channel import MarketingChannelModel
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.seeder.chaos import apply_campaign_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_campaign_payloads(active_channels: list[dict], chaos_rate: float = 0.05, apply_chaos: bool = True) -> list[dict]:
    """
    Generates campaign payloads based on REAL channels from the database.
    """
    batch = []
    campaign_id_counter = 1

    for channel in active_channels:
        num_campaigns = random.randint(1, 4)
        for i in range(num_campaigns):
            platform = channel["platform_name"]
            is_dynamic = random.choice([True, False])
            locations = random.sample(["Brazil", "United States", "Portugal", "Global"], k=random.randint(1, 2))
            devices = random.sample(["Mobile", "Desktop", "Tablet"], k=random.randint(1, 3))

            cpa, roas = None, None
            if platform == "google_ads":
                campaign_name = f"GSN_{fake.word().capitalize()}_{random.choice(['Brand', 'Generic'])}"
                objective = random.choice(["SALES", "LEADS"])
                bidding = random.choice(["TARGET_ROAS", "MAXIMIZE_CONVERSIONS"])
                if bidding == "TARGET_ROAS": roas = round(random.uniform(3.0, 7.0), 2)
                audience = "In-Market Buyers"
            elif platform == "meta_ads":
                campaign_name = f"FB_IG_{fake.word().capitalize()}_{random.choice(['Retargeting', 'Prospecting'])}"
                objective = "CONVERSIONS"
                bidding = "COST_PER_RESULT_GOAL"
                cpa = round(random.uniform(10.0, 60.0), 2)
                audience = "Lookalike 1% Purchasers"
            else:
                campaign_name = f"{platform.upper()}_{fake.word()}_Promo"
                objective = "AWARENESS"
                bidding = "MAX_IMPRESSIONS"
                audience = "General Interest"

            status = random.choice(list(CampaignStatus))
            start_date = datetime.now() - timedelta(days=random.randint(30, 365))
            end_date = start_date + timedelta(days=random.randint(30, 90)) if status == CampaignStatus.ACTIVE else None

            payload = {
                "campaign_id": campaign_id_counter,  # placeholder for Pydantic validation (gt=0); removed before insert
                "channel_id": channel["channel_id"],
                "campaign_name": campaign_name,
                "utm_campaign": f"utm_campaign={campaign_name.lower()}",
                "budget_type": random.choice(list(MarketingBudgeType)),
                "total_budget": round(random.uniform(1000.0, 30000.0), 2),
                "start_date": start_date,
                "end_date": end_date,
                "status": status,
                "mktsettings": {
                    "target_audience": audience,
                    "objective": objective,
                    "conversion_goal": "PURCHASE",
                    "bidding_strategy": bidding,
                    "target_roas": roas,
                    "target_cpa": cpa,
                    "location_targeting": locations,
                    "device_targeting": devices,
                    "is_dynamic_creative": is_dynamic
                },
                "created_at": start_date,
                "updated_at": datetime.now(),
                "is_active": status == CampaignStatus.ACTIVE,
                "row_guid": uuid.uuid4()
            }
            campaign_obj = MarketingCampaign(**payload)
            validated_campaign = campaign_obj.model_dump()
            # mktsettings is JSONB — Decimal (target_roas/target_cpa) needs to become a JSON-safe type
            validated_campaign["mktsettings"] = campaign_obj.mktsettings.model_dump(mode="json")
            validated_campaign.pop("campaign_id", None)  # let the database's autoincrement take over
            batch.append(validated_campaign)
            campaign_id_counter += 1

    if apply_chaos:
        batch = apply_campaign_chaos(batch, chaos_rate=chaos_rate)

    return batch

async def seed_marketing_campaigns(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """
    Runs the campaign load consuming REAL Channels from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch real channels (Dimensional Integrity)
        result = await session.execute(select(MarketingChannelModel.channel_id, MarketingChannelModel.platform_name))
        real_channels = [{"channel_id": row[0], "platform_name": row[1]} for row in result.all()]

        if not real_channels:
            print("⚠️ No channel found! Run the MarketingChannel seeder first.")
            return

        print(f"📣 Seeding campaigns for {len(real_channels)} active channels | chaos={chaos_rate:.2%}")
        campaigns = generate_campaign_payloads(real_channels, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            stmt = insert(MarketingCampaignModel).values(campaigns)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(campaigns)} campaigns seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding campaigns: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_campaign (fan-out of real channels)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_marketing_campaigns(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
