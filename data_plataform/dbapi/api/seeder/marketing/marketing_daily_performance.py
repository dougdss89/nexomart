import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta, date
from decimal import Decimal
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_daily_performance import MarketingDailyPerformance, MarketingMetrics
from api.models.marketing.marketing_advertise import MarketingAdsModel
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_performance_payloads(ads_reais: list[dict]) -> list[dict]:
    """
    Gera a Tabela Fato de Performance Diária com funil matemático.
    """
    batch = []
    data_hoje = datetime.now().date()
    
    for an in ads_reais:
        # Gera de 7 a 30 dias de histórico
        dias_historico = random.randint(7, 30)
        data_inicio = data_hoje - timedelta(days=dias_historico)
        
        is_gsn = "GSN" in an["ad_name"].upper()
        media_type = an["creative_data"].get("media_type", "image")

        for j in range(dias_historico + 1):
            dia_ref = data_inicio + timedelta(days=j)
            
            # --- Funil Matemático ---
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
                "performance_id": None, # Auto-increment
                "campaign_id": an["campaign_id"],
                "ad_group_id": an["ad_group_id"],
                "ad_id": an["ad_id"],
                "date_ref": dia_ref,
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
            batch.append(MarketingDailyPerformance(**payload).model_dump())
            
    return batch

async def seed_marketing_performance():
    """
    Executa a carga de performance consumindo a hierarquia REAL do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar a malha de anúncios -> adgroups -> campaigns
        query = (
            select(
                MarketingAdsModel.ad_id,
                MarketingAdsModel.ad_name,
                MarketingAdsModel.creative_data,
                MarketingAdsModel.ad_group_id,
                MarketingAdGroupModel.campaign_id
            )
            .join(MarketingAdGroupModel, MarketingAdsModel.ad_group_id == MarketingAdGroupModel.ad_group_id)
        )
        result = await session.execute(query)
        ads_reais = [
            {
                "ad_id": row[0],
                "ad_name": row[1],
                "creative_data": row[2],
                "ad_group_id": row[3],
                "campaign_id": row[4]
            }
            for row in result.all()
        ]

        if not ads_reais:
            print("⚠️ Nenhum Anúncio encontrado! Rode os seeders de marketing anteriores primeiro.")
            return

        print(f"📣 Semeando performance diária para {len(ads_reais)} anúncios...")
        performances = generate_performance_payloads(ads_reais)
        
        # Inserção em lotes de 1000 para evitar timeout ou estouro de buffer
        batch_size = 1000
        for i in range(0, len(performances), batch_size):
            current_batch = performances[i:i+batch_size]
            try:
                stmt = insert(MarketingDailyPerformanceModel).values(current_batch)
                await session.execute(stmt)
                print(f"✅ Inserindo lote {i//batch_size + 1} ({len(current_batch)} linhas)...")
            except Exception as e:
                print(f"❌ Erro no lote: {e}")
                break
        
        await session.commit()
        print(f"🚀 Seeder de Performance finalizado com sucesso!")

if __name__ == "__main__":
    asyncio.run(seed_marketing_performance())
