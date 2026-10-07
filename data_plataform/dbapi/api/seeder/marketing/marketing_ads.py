import uuid
import random
import asyncio
import sys
import os
from datetime import datetime
from faker import Faker
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_ads import MarketingAd, MarketingStatus, MarketingMediaType
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.models.marketing.marketing_advertise import MarketingAdvertiseModel
from api.seeder.chaos import apply_ads_chaos
from api.utils.cli_common import build_parser, add_chaos_args, resolve_chaos_args, insert_in_batches

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_ad_payloads(real_adgroups: list[dict], chaos_rate: float = 0.05, apply_chaos: bool = True) -> list[dict]:
    """
    Generates Ad payloads based on REAL AdGroups from the database.
    """
    batch = []
    ad_id_counter = 1
    for adgroup in real_adgroups:
        num_ads = random.randint(1, 3)
        adgroup_name = adgroup["ad_group_name"]
        
        for i in range(num_ads):
            if "GSN" in adgroup_name.upper():
                media_type = MarketingMediaType.TEXT
                headlines = ["Promoção Exclusiva", "Melhor Preço do Mercado"]
            elif "FB_IG" in adgroup_name.upper() or "TIKTOK" in adgroup_name.upper():
                media_type = random.choice([MarketingMediaType.IMAGE, MarketingMediaType.VIDEO])
                headlines = ["Você não vai acreditar", "O segredo dos experts"]
            else:
                media_type = MarketingMediaType.IMAGE
                headlines = ["Confira nossa oferta"]

            media_assets = []
            if media_type == MarketingMediaType.VIDEO:
                media_assets.append({"url": f"https://cdn.example.com/ad_{uuid.uuid4().hex[:6]}.mp4", "duration_seconds": 30})
            elif media_type == MarketingMediaType.IMAGE:
                media_assets.append({"url": f"https://cdn.example.com/ad_{uuid.uuid4().hex[:6]}.jpg"})

            status = random.choice(list(MarketingStatus))
            payload = {
                "ad_id": ad_id_counter,  # placeholder for Pydantic validation (gt=0); removed before insert
                "ad_group_id": adgroup["ad_group_id"],
                "ad_name": f"AD_{i+1}_{adgroup_name}",
                "external_id": f"EXT-AD-{uuid.uuid4().hex[:10].upper()}",
                "utm_content": f"ad-{uuid.uuid4().hex[:4]}",
                "utm_medium": "cpc",
                "ad_status": status,
                "creative_data": {
                    "headline": random.choice(headlines),
                    "description": fake.text(max_nb_chars=150),
                    "final_url": "https://www.example.com/lp",
                    "media_type": media_type,
                    "media_assets": media_assets
                },
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": status == MarketingStatus.ACTIVE,
                "row_guid": uuid.uuid4()
            }
            ad_obj = MarketingAd(**payload)
            validated_ad = ad_obj.model_dump()
            # creative_data is JSONB — HttpUrl/AnyUrl need to become strings (JSON-safe type)
            validated_ad["creative_data"] = ad_obj.creative_data.model_dump(mode="json")
            validated_ad.pop("ad_id", None)  # let the database's autoincrement take over
            batch.append(validated_ad)
            ad_id_counter += 1

    if apply_chaos:
        batch = apply_ads_chaos(batch, chaos_rate=chaos_rate)

    return batch

async def seed_marketing_ads(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """
    Runs the Ads load consuming REAL AdGroups from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch AdGroups that don't have an Ad yet (same idempotency/batching rationale
        # as marketing_adgroup.py: reprocessing an AdGroup that already has Ads just piles
        # on more without violating any constraint, but grows unbounded every run and
        # eventually blows past asyncpg's 32767-param limit via the unbatched INSERT).
        already_has_ad = set(
            (await session.execute(select(MarketingAdvertiseModel.ad_group_id.distinct()))).scalars().all()
        )
        result = await session.execute(select(MarketingAdGroupModel.ad_group_id, MarketingAdGroupModel.ad_group_name))
        real_adgroups = [
            {"ad_group_id": row[0], "ad_group_name": row[1]}
            for row in result.all() if row[0] not in already_has_ad
        ]

        if not real_adgroups:
            print("⚠️ No AdGroup pending an Ad! Run the MarketingAdGroup seeder first (or everyone already has one).")
            return

        print(f"📣 Seeding Ads for {len(real_adgroups)} groups pending one | chaos={chaos_rate:.2%}")
        ads = generate_ad_payloads(real_adgroups, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await insert_in_batches(session, MarketingAdvertiseModel, ads)
            print(f"✅ {total} Ads seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding Ads: {e}")
            raise RuntimeError(f"Marketing ads seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = build_parser("Seeder for marketing.marketing_advertise (fan-out of real adgroups)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_marketing_ads(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
