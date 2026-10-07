import uuid
import random
import asyncio
import sys
import os
from datetime import datetime
from faker import Faker
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_adgroup import MarketingAdGroup, DeviceType
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.samples.person_demographics import INTERESTS_BANK, OCCUPATIONS
from api.seeder.chaos import apply_adgroup_chaos
from api.utils.cli_common import build_parser, add_chaos_args, resolve_chaos_args, insert_in_batches

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_adgroup_payloads(real_campaigns: list[dict], chaos_rate: float = 0.05, apply_chaos: bool = True) -> list[dict]:
    """
    Generates AdGroup payloads based on REAL campaigns from the database.
    """
    batch = []
    ad_group_id_counter = 1
    for campaign in real_campaigns:
        num_adgroups = random.randint(1, 4)
        for i in range(num_adgroups):
            adgroup_interests = random.sample(INTERESTS_BANK, k=random.randint(1, 3))
            adgroup_keywords = random.sample(OCCUPATIONS, k=random.randint(2, 4))
            adgroup_keywords.extend([f"comprar {fake.word()}", "oferta exclusiva"])

            min_age = random.randint(18, 45)

            payload = {
                "ad_group_id": ad_group_id_counter,  # placeholder for Pydantic validation (gt=0); removed before insert
                "campaign_id": campaign["campaign_id"],
                "ad_group_name": f"AG_{i+1}_{campaign['campaign_name']}_{adgroup_interests[0][:5].upper()}",
                "external_id": f"EXT-ADG-{uuid.uuid4().hex[:8].upper()}",
                "targeting_criteria": {
                    "geo_location": {"include": ["Brasil"], "exclude": []},
                    "age_range": {"min_age": min_age, "max_age": min_age + 20},
                    "interests": adgroup_interests,
                    "keywords": adgroup_keywords,
                    "device_types": [random.choice(list(DeviceType))],
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
            validated_adgroup = MarketingAdGroup(**payload).model_dump()
            validated_adgroup.pop("ad_group_id", None)  # let the database's autoincrement take over
            batch.append(validated_adgroup)
            ad_group_id_counter += 1

    if apply_chaos:
        batch = apply_adgroup_chaos(batch, chaos_rate=chaos_rate)

    return batch

async def seed_marketing_adgroups(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """
    Runs the AdGroup load consuming REAL Campaigns from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch campaigns that don't have an AdGroup yet — reprocessing a campaign that
        # already has AdGroups just piles on more of them without violating any constraint
        # (ad_group_id is autoincrement), but grows without bound on every DAG run for no
        # reason, and eventually the unbatched INSERT below blows past asyncpg's 32767-param
        # limit once enough campaigns accumulate (incident 2026-08-13: 2,511 campaigns x
        # ~2.5 adgroups/campaign x 8 columns exceeded it in one shot).
        already_has_adgroup = set(
            (await session.execute(select(MarketingAdGroupModel.campaign_id.distinct()))).scalars().all()
        )
        result = await session.execute(select(MarketingCampaignModel.campaign_id, MarketingCampaignModel.campaign_name))
        real_campaigns = [
            {"campaign_id": row[0], "campaign_name": row[1]}
            for row in result.all() if row[0] not in already_has_adgroup
        ]

        if not real_campaigns:
            print("⚠️ No campaign pending an AdGroup! Run the MarketingCampaign seeder first (or everyone already has one).")
            return

        print(f"📣 Seeding AdGroups for {len(real_campaigns)} campaigns pending one | chaos={chaos_rate:.2%}")
        adgroups = generate_adgroup_payloads(real_campaigns, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await insert_in_batches(session, MarketingAdGroupModel, adgroups)
            print(f"✅ {total} AdGroups seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding AdGroups: {e}")
            raise RuntimeError(f"AdGroup seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_adgroup (fan-out of real campaigns)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_marketing_adgroups(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
