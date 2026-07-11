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

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_ad_payloads(adgroups_reais: list[dict]) -> list[dict]:
    """
    Gera payloads de Anúncios baseados em AdGroups REAIS do banco.
    """
    batch = []
    for adgroup in adgroups_reais:
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
                "ad_id": None, # Auto-increment
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
            batch.append(MarketingAd(**payload).model_dump())

    return apply_ads_chaos(batch)

async def seed_marketing_ads():
    """
    Executa a carga dos Anúncios consumindo AdGroups REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar AdGroups reais (Dimensional Integrity)
        result = await session.execute(select(MarketingAdGroupModel.ad_group_id, MarketingAdGroupModel.ad_group_name))
        adgroups_reais = [{"ad_group_id": row[0], "ad_group_name": row[1]} for row in result.all()]

        if not adgroups_reais:
            print("⚠️ Nenhum AdGroup encontrado! Rode o seeder de MarketingAdGroup primeiro.")
            return

        print(f"📣 Semeando Anúncios para {len(adgroups_reais)} grupos ativos...")
        ads = generate_ad_payloads(adgroups_reais)
        
        try:
            stmt = insert(MarketingAdvertiseModel).values(ads)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(ads)} Anúncios semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear Anúncios: {e}")

if __name__ == "__main__":
    asyncio.run(seed_marketing_ads())
