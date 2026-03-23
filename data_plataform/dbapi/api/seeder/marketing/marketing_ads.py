import uuid
import random
from datetime import datetime
from faker import Faker

from api.schemas.marketing.marketing_ads import MarketingAd, MarketingStatus, MarketingMediaType
from api.seeder.marketing.marketing_adgroup import get_marketing_adgroup_seeders

fake = Faker('pt_BR')

def get_marketing_ads_seeders() -> list[dict]:
    """
    Gera o último elo hierárquico: Os Anúncios e seus Criativos (Copys e Mídias).
    Deriva a Mídia (Video, Data, Carousel) inteligentemente baseada na Plataforma detectada através do naming convention do AdGroup.
    """
    batch = []
    ad_counter_id = 1
    
    # Busca a cadeia anterior
    adgroups_gerados = get_marketing_adgroup_seeders()
    
    for adgroup in adgroups_gerados:
        
        # Sorteia de 1 a 4 Anúncios dentro deste AdGroup (ex: teste A/B/C)
        num_ads = random.randint(1, 4)
        adgroup_name = adgroup["ad_group_name"]
        
        for i in range(num_ads):
            
            # --- Detecta Lógica da Plataforma pelo Root Source ---
            # Ex: AdGroup_1_GSN_...
            # Ex: AdGroup_1_FB_IG_...
            if "_GSN_" in adgroup_name: # Google Search Network
                media_type = MarketingMediaType.TEXT
                ad_name = f"Ad_Text_Copy{i+1}"
                headlines = ["Acelere seu Crescimento", "Desconto Black Friday", "Compre agora com 50% Off!"]
            elif "_FB_IG_" in adgroup_name: # Meta Ads
                media_type = random.choice([MarketingMediaType.IMAGE, MarketingMediaType.VIDEO, MarketingMediaType.CAROUSEL])
                ad_name = f"Ad_{media_type.value.capitalize()}_Creative{i+1}"
                headlines = ["Conheça nossa nova coleção", "O segredo revelado", "Veja por que estão comprando"]
            elif "TIKTOK" in adgroup_name.upper():
                media_type = MarketingMediaType.VIDEO
                ad_name = f"Ad_UGC_Video_{i+1}"
                headlines = ["Pov: Você achou o produto perfeito", "Testando o hype do momento"]
            elif "LINKEDIN" in adgroup_name.upper():
                media_type = random.choice([MarketingMediaType.IMAGE, MarketingMediaType.TEXT])
                ad_name = f"Ad_B2B_Prospect_{i+1}"
                headlines = ["Escale sua Operação 10x", "Software Enterprise Completo"]
            else:
                media_type = random.choice(list(MarketingMediaType))
                ad_name = f"Ad_Generic_{i+1}"
                headlines = ["Veja nossa oferta", "Assine agora"]
                
            headline_escolhida = random.choice(headlines)
            
            # --- Gerar Payload do Creative (Mídias e URLs) ---
            media_assets = []
            
            if media_type == MarketingMediaType.VIDEO:
                media_assets.append({
                    "url": f"https://cdn.empresa.com.br/video/ad_{uuid.uuid4().hex[:6]}.mp4",
                    "duration_seconds": random.choice([15, 30, 60]),
                    "file_size_mb": round(random.uniform(5.0, 50.0), 2),
                    "width": 1080,
                    "height": random.choice([1920, 1080]) # Vertical (Reels) ou Square
                })
            elif media_type in [MarketingMediaType.IMAGE, MarketingMediaType.CAROUSEL]:
                num_images = random.randint(2, 5) if media_type == MarketingMediaType.CAROUSEL else 1
                for _ in range(num_images):
                    media_assets.append({
                        "url": f"https://cdn.empresa.com.br/img/ad_banner_{uuid.uuid4().hex[:6]}.jpg",
                        "width": 1080,
                        "height": 1080, # Quadrada
                        "file_size_mb": round(random.uniform(0.5, 3.0), 2)
                    })
            else: # Text não possui MediaAsset visuais
                pass
                
            # Regras Herança Temporal
            criado_em = adgroup["created_at"]
            status = random.choice(list(MarketingStatus))
            
            payload = {
                "ad_id": ad_counter_id,
                "ad_group_id": adgroup["ad_group_id"],
                "ad_name": f"{adgroup_name}_{ad_name}",
                "external_id": f"EXT-AD-{ad_counter_id:08d}",
                
                "utm_content": f"ad-{ad_counter_id}",
                "utm_medium": "cpc",
                
                "ad_status": status,
                
                "creative_data": {
                    "headline": headline_escolhida,
                    "description": fake.text(max_nb_chars=180),
                    "final_url": f"https://www.empresa.com.br/lp?variant={uuid.uuid4().hex[:4]}",
                    "media_type": media_type,
                    "media_assets": media_assets
                },
                
                "created_at": criado_em,
                "updated_at": datetime.now(),
                "is_active": status == MarketingStatus.ACTIVE,
                "row_guid": str(uuid.uuid4())
            }
            
            # Validador Rigoroso Pydantic
            ad_validado = MarketingAd(**payload).model_dump()
            batch.append(ad_validado)
            
            ad_counter_id += 1
            
    return batch


if __name__ == "__main__":
    import json
    anuncios = get_marketing_ads_seeders()
    
    print(f"Total de Anúncios Simulados Mapeados c/ Sucesso: {len(anuncios)}")
    print("-" * 50)
    
    # Separação por tipo de Mídia
    videos = [a for a in anuncios if a["creative_data"]["media_type"] == "video"]
    texts = [a for a in anuncios if a["creative_data"]["media_type"] == "text"]
    
    if videos:
        print("\n--- AMOSTRA ANÚNCIO DE VÍDEO (Meta / TikTok) ---")
        v = videos[0]
        print(f"Nome do Anúncio (Derivação Base): {v['ad_name']}")
        print(f"Headeline do Criativo: {v['creative_data']['headline']}")
        print(f"URL de Destino Final: {v['creative_data']['final_url']}")
        print(f"CDN Link Mídia: {v['creative_data']['media_assets'][0]['url']}")
        print(f"Resolução Vídeo (AxL): {v['creative_data']['media_assets'][0]['height']}x{v['creative_data']['media_assets'][0]['width']} pixels")
        
    if texts:
        print("\n--- AMOSTRA ANÚNCIO DE TEXTO (Google Search) ---")
        t = texts[0]
        print(f"Nome do Ad: {t['ad_name']}")
        print(f"Copy: {t['creative_data']['description'][:50]}...")
        
    print("\n\n--- DUMP JSONB ---")
    if anuncios:
        print(json.dumps(anuncios[0], indent=2, default=str))
