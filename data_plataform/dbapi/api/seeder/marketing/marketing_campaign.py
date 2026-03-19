import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

from api.schemas.marketing.marketing_campaign import MarketingCampaign, MarketingBudgeType, CampaignStatus
from api.seeder.marketing.marketing_channel import get_marketing_channel_seeders

fake = Faker('pt_BR')

def get_marketing_campaign_seeders() -> list[dict]:
    """
    Gera as Campanhas de Marketing atreladas aos Canais Dimensões.
    Simula payloads reais extraídos de plataformas como Google Ads e Meta Ads.
    """
    batch = []
    campaign_counter_id = 1
    
    # 1. Obter a Tabela Verdade de Canais
    canais_disponiveis = get_marketing_channel_seeders()
    
    # Vamos gerar campanhas apenas para plataformas pagas/gerenciadas
    plataformas_pagas = ["google_ads", "meta_ads", "tiktok", "linkedin_ads", "mailchimp"]
    canais_ativos = [c for c in canais_disponiveis if c["platform_name"].lower() in plataformas_pagas]
    
    for canal in canais_ativos:
        # Gerar de 1 a 3 campanhas por Canal
        num_campanhas = random.randint(1, 4)
        
        for i in range(num_campanhas):
            plataforma = canal["platform_name"]
            
            # --- Defaults/Base Setup Mocks ---
            is_dynamic = random.choice([True, False])
            locations = random.sample(["Brazil", "United States", "Portugal", "Global", "LatAm"], k=random.randint(1, 2))
            devices = random.sample(["Mobile", "Desktop", "Tablet"], k=random.randint(1, 3))
            languages = random.sample(["PT-BR", "EN-US", "ES"], k=random.randint(1, 2))
            placements = []
            ad_schedule = random.choice([None, "Mon-Fri 09:00-18:00", "Weekends Only", "24/7"])
            cpa = None
            roas = None
            
            # --- Lógica de Simulação Específica por Plataforma ---
            if plataforma == "google_ads":
                campaign_name = f"GSN_{fake.word().capitalize()}_{random.choice(['Brand', 'Generic', 'Competitors'])}"
                utm = f"utm_campaign={campaign_name.lower().replace('_', '-')}"
                objective = random.choice(["SALES", "LEADS", "WEBSITE_TRAFFIC"])
                bidding = random.choice(["TARGET_ROAS", "MAXIMIZE_CONVERSIONS", "TARGET_CPA"])
                
                if bidding == "TARGET_ROAS":
                    roas = round(random.uniform(2.5, 8.0), 2)
                elif bidding == "TARGET_CPA":
                    cpa = round(random.uniform(15.0, 150.0), 2)
                    
                audience = "In-Market Buyers"
                placements = ["Search Network", "Display Network", "YouTube"] if is_dynamic else ["Search Network"]
            
            elif plataforma == "meta_ads":
                campaign_name = f"FB_IG_{fake.word().capitalize()}_{random.choice(['Retargeting', 'Prospecting', 'Lookalike'])}"
                utm = f"utm_campaign={campaign_name.lower().replace('_', '-')}"
                objective = random.choice(["CONVERSIONS", "LEAD_GENERATION", "BRAND_AWARENESS"])
                bidding = random.choice(["HIGHEST_VOLUME", "COST_PER_RESULT_GOAL"])
                
                if bidding == "COST_PER_RESULT_GOAL":
                    cpa = round(random.uniform(5.0, 80.0), 2)
                    
                audience = random.choice(["Lookalike 1% Purchasers", "Broad (18-35)", "Retargeting WCA 30d"])
                placements = ["Instagram Stories", "Facebook Feed", "Instagram Reels"] if not is_dynamic else ["Advantage+ Placements"]
            
            elif plataforma == "mailchimp":
                campaign_name = f"NL_Promo_{fake.word().capitalize()}_{datetime.now().strftime('%Y%m')}"
                utm = f"utm_campaign=email-{datetime.now().strftime('%Y%m')}"
                objective = "RETENTION"
                bidding = "FIXED_COST"
                audience = "Active Subscribers"
                placements = ["Inbox", "Promotions Tab"]
                is_dynamic = False
                
            else: # Tiktok / Linkedin
                campaign_name = f"{plataforma.upper()}_{fake.word()}_Awareness"
                utm = f"utm_campaign={plataforma}-promo"
                objective = "AWARENESS"
                bidding = "MAX_IMPRESSIONS"
                audience = "B2B Professionals" if plataforma == "linkedin_ads" else "Gen Z Target"
                placements = ["LinkedIn Feed", "LinkedIn Messages"] if plataforma == "linkedin_ads" else ["TikTok For You"]
            
            status = random.choice(list(CampaignStatus))
            
            # Lógica Temporal: Se for Completed, tem que ter end_date < Hoje
            data_hoje = datetime.now()
            start_date = data_hoje - timedelta(days=random.randint(10, 365))
            
            if status in [CampaignStatus.COMPLETED, CampaignStatus.CANCELLED]:
                end_date = start_date + timedelta(days=random.randint(5, 30))
            elif status == CampaignStatus.ACTIVE:
                # Pode ter ou não ter end_date no futuro
                end_date = start_date + timedelta(days=random.randint(30, 90)) if random.random() > 0.5 else None
            else:
                end_date = None
                
            payload = {
                "campaign_id": campaign_counter_id,
                "channel_id": canal["channel_id"],
                "campaign_name": campaign_name,
                "utm_campaign": utm,
                "budget_type": random.choice(list(MarketingBudgeType)),
                "total_budget": round(random.uniform(500.0, 50000.0), 2),
                "start_date": start_date,
                "end_date": end_date,
                "status": status,
                "mktsettings": {
                    "target_audience": audience,
                    "objective": objective,
                    "conversion_goal": "PURCHASE" if objective in ["SALES", "CONVERSIONS"] else "SUBMIT_FORM",
                    "bidding_strategy": bidding,
                    "target_roas": roas,
                    "target_cpa": cpa,
                    "promoted_product_categories": random.sample(["Electronics", "Home", "Apparel", "Software"], k=random.randint(1, 2)),
                    # Novos atributos enriquecidos
                    "location_targeting": locations,
                    "device_targeting": devices,
                    "language_targeting": languages,
                    "placement_inclusions": placements,
                    "ad_schedule": ad_schedule,
                    "is_dynamic_creative": is_dynamic
                },
                "created_at": start_date,
                "updated_at": end_date if end_date else start_date,
                "is_active": status == CampaignStatus.ACTIVE,
                "row_guid": str(uuid.uuid4())
            }
            
            # Validação PyDantic Rigorosa
            campanha_validada = MarketingCampaign(**payload).model_dump()
            batch.append(campanha_validada)
            
            campaign_counter_id += 1
            
    return batch


if __name__ == "__main__":
    import json
    campanhas = get_marketing_campaign_seeders()
    print(f"Total de Campanhas simuladas geradas c/ sucesso: {len(campanhas)}\n")
    print("-" * 50)
    
    google = next((c for c in campanhas if "GSN" in c["campaign_name"]), None)
    meta = next((c for c in campanhas if "FB_IG" in c["campaign_name"]), None)
    
    if google:
        print("\n--- GOOGLE ADS SIMULATION ---")
        print(f"Nome: {google['campaign_name']} | Bidding: {google['mktsettings']['bidding_strategy']} | Target ROAS: {google['mktsettings']['target_roas']}")
    
    if meta:
        print("\n--- META ADS SIMULATION ---")
        print(f"Nome: {meta['campaign_name']} | Objetivo: {meta['mktsettings']['objective']} | Audience: {meta['mktsettings']['target_audience']}")
        
    print("\n--- AMOSTRA DE PAYLOAD (JSON) ---")
    print(json.dumps(campanhas[0], indent=2, default=str))
