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

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_campaign_payloads(canais_ativos: list[dict]) -> list[dict]:
    """
    Gera payloads de campanhas baseados em canais REAIS do banco.
    """
    batch = []
    
    for canal in canais_ativos:
        num_campanhas = random.randint(1, 4)
        for i in range(num_campanhas):
            plataforma = canal["platform_name"]
            is_dynamic = random.choice([True, False])
            locations = random.sample(["Brazil", "United States", "Portugal", "Global"], k=random.randint(1, 2))
            devices = random.sample(["Mobile", "Desktop", "Tablet"], k=random.randint(1, 3))
            
            cpa, roas = None, None
            if plataforma == "google_ads":
                campaign_name = f"GSN_{fake.word().capitalize()}_{random.choice(['Brand', 'Generic'])}"
                objective = random.choice(["SALES", "LEADS"])
                bidding = random.choice(["TARGET_ROAS", "MAXIMIZE_CONVERSIONS"])
                if bidding == "TARGET_ROAS": roas = round(random.uniform(3.0, 7.0), 2)
                audience = "In-Market Buyers"
            elif plataforma == "meta_ads":
                campaign_name = f"FB_IG_{fake.word().capitalize()}_{random.choice(['Retargeting', 'Prospecting'])}"
                objective = "CONVERSIONS"
                bidding = "COST_PER_RESULT_GOAL"
                cpa = round(random.uniform(10.0, 60.0), 2)
                audience = "Lookalike 1% Purchasers"
            else:
                campaign_name = f"{plataforma.upper()}_{fake.word()}_Promo"
                objective = "AWARENESS"
                bidding = "MAX_IMPRESSIONS"
                audience = "General Interest"

            status = random.choice(list(CampaignStatus))
            start_date = datetime.now() - timedelta(days=random.randint(30, 365))
            end_date = start_date + timedelta(days=random.randint(30, 90)) if status == CampaignStatus.ACTIVE else None

            payload = {
                "campaign_id": None, # Auto-increment
                "channel_id": canal["channel_id"],
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
            batch.append(MarketingCampaign(**payload).model_dump())

    return apply_campaign_chaos(batch)

async def seed_marketing_campaigns(count: int = 20):
    """
    Executa a carga das campanhas consumindo Canais REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar canais reais (Dimensional Integrity)
        result = await session.execute(select(MarketingChannelModel.channel_id, MarketingChannelModel.platform_name))
        canais_reais = [{"channel_id": row[0], "platform_name": row[1]} for row in result.all()]

        if not canais_reais:
            print("⚠️ Nenhum canal encontrado! Rode o seeder de MarketingChannel primeiro.")
            return

        print(f"📣 Semeando campanhas para {len(canais_reais)} canais ativos...")
        campanhas = generate_campaign_payloads(canais_reais)
        
        try:
            stmt = insert(MarketingCampaignModel).values(campanhas)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(campanhas)} campanhas semeadas com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear campanhas: {e}")

if __name__ == "__main__":
    asyncio.run(seed_marketing_campaigns())
