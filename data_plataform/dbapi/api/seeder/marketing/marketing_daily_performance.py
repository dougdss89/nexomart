import uuid
import random
from datetime import datetime, timedelta, date
from decimal import Decimal

from api.schemas.marketing.marketing_daily_performance import MarketingDailyPerformance, MarketingMetrics
from api.seeder.marketing.marketing_ads import get_marketing_ads_seeders
from api.seeder.marketing.marketing_adgroup import get_marketing_adgroup_seeders

def get_marketing_daily_performance_seeders() -> list[dict]:
    """
    Gera a Tabela Fato de Performance Diária (Data Warehouse).
    Simula perfeitamente algoritmos de funil matemático para Marketing Digital.
      1. Imprime os Anúncios via impressões.
      2. Destila o CTR (1% a 6%) gerando Cliques.
      3. Gera o Custo baseado num CPC ($0.20 a $5.00).
      4. Destila o CVR de Site (1% a 8%) gerando Conversões.
      5. Aplica Ticket Médio Específico ($30 a $400) e gera Revenue.
      6. Se for Vídeo adiciona Quartis (25%-100%). Se Social (Likes), Se Google (Quality Score).
    """
    batch = []
    perf_counter_id = 1
    
    # Construir mapa de herança (AdGroup -> Campaign)
    adgroups = get_marketing_adgroup_seeders()
    map_adgroup_campaign = {ag["ad_group_id"]: ag["campaign_id"] for ag in adgroups}
    
    # Array gigante importado de Anúncios finalizados
    anuncios_gerados = get_marketing_ads_seeders()
    
    data_hoje = datetime.now()
    data_hoje_date = data_hoje.date()
    
    for an in anuncios_gerados:
        
        # Qual é o histórico temporal?
        data_inicio = an["created_at"].date()
        
        # Se ativo forçar 30 dias se criacao > 30, senão usar até hoje.
        # Se inativo, terminar na dt_update original ou 7 dias pos inicio.
        
        if an["is_active"]:
            dias_idade = (data_hoje_date - data_inicio).days
            data_fim = data_hoje_date
            # Previnir geração infinita (vamos mockar os últimos 30-60 dias estourando)
            if dias_idade > 30:
                data_inicio = data_hoje_date - timedelta(days=30)
        else:
            data_fim = an.get("updated_at", data_hoje).date()
            if data_fim <= data_inicio: 
                data_fim = data_inicio + timedelta(days=random.randint(5, 15))
                
        dias_ativos = (data_fim - data_inicio).days
        if dias_ativos < 1:
            dias_ativos = 1
            
        media_type = an["creative_data"]["media_type"]
        is_gsn = "GSN" in an["ad_name"]
        
        # Iterar linha do tempo diária
        for j in range(dias_ativos + 1):
            dia_ref = data_inicio + timedelta(days=j)
            
            # --- Funil Matemático (Data Science Base) ---
            # Impressões flutuam aleatoriamente todo dia (Sazonalidade fraca)
            impressions = random.randint(50, 8000)
            
            # CTR flutua. Vídeo atrai mais CTR, GS atrai menos volume mas conversão maior.
            ctr = random.uniform(0.01, 0.06) 
            clicks = int(impressions * ctr)
            
            # CPC = (Custo / Clicks)
            cpc = round(random.uniform(0.15, 4.50), 4)
            cost = round(clicks * cpc, 4)
            
            # CVR (Site Conversion Rate) das pessoas que clicam versus finalizam
            cvr = random.uniform(0.015, 0.085)
            conversions = int(clicks * cvr)
            
            # Valor Médio de Pedido (AOV)
            aov = round(random.uniform(50.0, 350.0), 2)
            conversion_value = round(conversions * aov, 2)
            
            # --- Métricas Específicas Avançadas JSONB ---
            metrics_payload = {}
            
            # Video quartiles (Funil de View Rate)
            if media_type == "video":
                v_views = int(impressions * random.uniform(0.2, 0.4))
                metrics_payload["video_views"] = v_views
                metrics_payload["video_views_25"] = int(v_views * 0.8)
                metrics_payload["video_views_50"] = int(v_views * 0.4)
                metrics_payload["video_views_75"] = int(v_views * 0.2)
                metrics_payload["video_views_100"] = int(v_views * 0.1)
                
            # Social Engagement
            if "FB_IG" in an["ad_name"] or "TIKTOK" in an["ad_name"]:
                metrics_payload["likes"] = random.randint(0, max(5, impressions // 10))
                metrics_payload["comments"] = random.randint(0, max(2, impressions // 100))
                metrics_payload["shares"] = random.randint(0, max(2, impressions // 80))
                metrics_payload["saves"] = random.randint(0, max(2, impressions // 60))
                metrics_payload["clicks_all"] = clicks + random.randint(1, 15)
                
            # Funil Granular (Add To Cart)
            add_to_cart = conversions + random.randint(0, conversions + 10)
            checkouts = conversions + random.randint(0, min(add_to_cart - conversions, 5))
            metrics_payload["adds_to_cart"] = add_to_cart
            metrics_payload["checkouts_initiated"] = checkouts
            
            # Google Ads Insights
            if is_gsn:
                metrics_payload["search_impression_share"] = round(random.uniform(0.3, 0.9), 4)
                metrics_payload["search_lost_is_budget"] = round(random.uniform(0.0, 0.5), 4)
                metrics_payload["search_lost_is_rank"] = round(random.uniform(0.0, 0.3), 4)
                metrics_payload["quality_score"] = random.randint(3, 10)
                
            # Alcance único vs Impressões brutas (Frequência = Imp / Reach)
            reach = int(impressions / random.uniform(1.2, 2.5))
            if reach < 1: reach = 1
            metrics_payload["reach"] = reach
            metrics_payload["frequency"] = round(impressions / reach, 2)
            
            payload = {
                "performance_id": perf_counter_id,
                "campaign_id": map_adgroup_campaign.get(an["ad_group_id"], 1),
                "ad_group_id": an["ad_group_id"],
                "ad_id": an["ad_id"],
                "date_ref": dia_ref,
                
                "cost": cost,
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "conversion_value": conversion_value,
                
                "metrics": metrics_payload,
                
                "created_at": dia_ref,
                "updated_at": dia_ref,
                "is_active": True,
                "row_guid": str(uuid.uuid4())
            }
            
            try:
                # O Pydantic irá sanitizar e preencher Defaults de métricas que faltaram com "0" ou None.
                perf_validada = MarketingDailyPerformance(**payload).model_dump()
                batch.append(perf_validada)
                perf_counter_id += 1
            except Exception as e:
                # Tolerância a falhas caso geremos data em branco
                print(f"Erro no Pydantic Performance do Ad {an['ad_id']}: {e}")
                
    return batch

if __name__ == "__main__":
    import json
    performances = get_marketing_daily_performance_seeders()
    
    print(f"Total de Linhas Analíticas Cúbicas (Time-Series) Fato: {len(performances)}")
    print("-" * 50)
    
    # Check Google and Social Logic
    social = [p for p in performances if p["metrics"]["likes"] > 0]
    google = [p for p in performances if p["metrics"]["search_impression_share"] is not None]
    
    if social:
        s = social[0]
        print(f"\n--- AMOSTRA SOCIAL MEDIA PAYLOAD ---")
        print(f"Data Base: {s['date_ref']} | Gastou: R${s['cost']}")
        print(f"Impressionou: {s['impressions']} | Likes: {s['metrics']['likes']}")
        print(f"Cliques: {s['clicks']} (Adicionaram ao Carrinho: {s['metrics']['adds_to_cart']}) => Converteram Final: {s['conversions']}")
        
    if google:
        g = google[0]
        print(f"\n--- AMOSTRA GOOGLE SEARCH PAYLOAD ---")
        print(f"Data Base: {g['date_ref']} | Quality Score do Leilão: {g['metrics']['quality_score']}/10")
        print(f"Gasto: R${g['cost']} gerou R${g['conversion_value']} de Receita (ROAS: {round(g['conversion_value']/max(g['cost'], Decimal(1)), 2)}x)")
        
    print("\n\n--- DUMP JSONB FATO ANALÍTICO ---")
    if performances:
        data = performances[0]
        # Converter Decimals/Dates para printar nativo JSON no terminal:
        data_serialized = json.dumps(data, indent=2, default=str)
        print(data_serialized)
