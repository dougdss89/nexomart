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

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_adgroup_payloads(campanhas_reais: list[dict]) -> list[dict]:
    """
    Gera payloads de AdGroups baseados em campanhas REAIS do banco.
    """
    batch = []
    for campanha in campanhas_reais:
        num_adgroups = random.randint(1, 4)
        for i in range(num_adgroups):
            adgroup_interests = random.sample(INTERESTS_BANK, k=random.randint(1, 3))
            adgroup_keywords = random.sample(OCCUPATIONS, k=random.randint(2, 4))
            adgroup_keywords.extend([f"comprar {fake.word()}", "oferta exclusiva"])
            
            min_age = random.randint(18, 45)
            
            payload = {
                "ad_group_id": None, # Auto-increment
                "campaign_id": campanha["campaign_id"],
                "ad_group_name": f"AG_{i+1}_{campanha['campaign_name']}_{adgroup_interests[0][:5].upper()}",
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
            batch.append(MarketingAdGroup(**payload).model_dump())
    return batch

async def seed_marketing_adgroups():
    """
    Executa a carga dos AdGroups consumindo Campanhas REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar Campanhas reais (Dimensional Integrity)
        result = await session.execute(select(MarketingCampaignModel.campaign_id, MarketingCampaignModel.campaign_name))
        campanhas_reais = [{"campaign_id": row[0], "campaign_name": row[1]} for row in result.all()]

        if not campanhas_reais:
            print("⚠️ Nenhuma campanha encontrada! Rode o seeder de MarketingCampaign primeiro.")
            return

        print(f"📣 Semeando AdGroups para {len(campanhas_reais)} campanhas...")
        adgroups = generate_adgroup_payloads(campanhas_reais)
        
        try:
            stmt = insert(MarketingAdGroupModel).values(adgroups)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(adgroups)} AdGroups semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear AdGroups: {e}")

if __name__ == "__main__":
    asyncio.run(seed_marketing_adgroups())
