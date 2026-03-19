import uuid
import random
from datetime import datetime
from faker import Faker

from api.schemas.marketing.marketing_adgroup import MarketingAdGroup, DeviceType
from api.seeder.marketing.marketing_campaign import get_marketing_campaign_seeders
from api.samples.person_demographics import INTERESTS_BANK, OCCUPATIONS

fake = Faker('pt_BR')

def get_marketing_adgroup_seeders() -> list[dict]:
    """
    Gera as divisões lógicas das Campanhas: Grupos de Anúncios.
    Aproveita o mock gerador de Pessoas/Clientes (Person) importando seus perfis comportamentais exatos
    para simular um AdGroup segmentado com altíssima taxa de match cruzado em Análise de Dados.
    """
    batch = []
    adgroup_counter_id = 1
    
    campanhas_geradas = get_marketing_campaign_seeders()
    
    for campanha in campanhas_geradas:
        
        # Sorteia de 1 a 3 AD Groups por Campanha
        num_adgroups = random.randint(1, 4)
        
        for i in range(num_adgroups):
            
            # --- Regras Temporais: Copia da Campanha ---
            criado_em = campanha["created_at"]
            is_active = campanha["is_active"]
            
            # --- Data Engineering: Cruzamento Semântico com Costumers ---
            adgroup_interests = random.sample(INTERESTS_BANK, k=random.randint(1, 4))
            
            # Usaremos o Keyword Array para caçar as "ocupações/profissões" e comportamentos
            adgroup_keywords = random.sample(OCCUPATIONS, k=random.randint(2, 5))
            # Adiciona genéricos
            adgroup_keywords.extend([f"comprar {fake.word()}", f"promoção {fake.word()}"])
            
            device_pool = list(DeviceType)
            # Regras Pydantic do Schema
            min_age = random.randint(18, 55)
            max_age = min_age + random.randint(5, 20)
            
            payload = {
                "ad_group_id": adgroup_counter_id,
                "campaign_id": campanha["campaign_id"],
                "ad_group_name": f"AdGroup_{i+1}_{campanha['campaign_name']}_{adgroup_interests[0][:5].upper()}",
                "external_id": f"EXT-ADG-{adgroup_counter_id:06d}",
                
                "targeting_criteria": {
                    "geo_location": {
                        "include": random.sample(["São Paulo", "Rio de Janeiro", "Minas Gerais", "Paraná", "Brasil Todo"], k=random.randint(1, 2)),
                        "exclude": ["Acre", "Roraima"] if random.random() > 0.8 else []
                    },
                    "age_range": {
                        "min_age": min_age,
                        "max_age": max_age
                    },
                    "interests": adgroup_interests,
                    "keywords": adgroup_keywords,
                    "device_types": random.sample(device_pool, k=random.randint(1, len(device_pool))),
                    "remarketing_list_ids": [f"AUD-{random.randint(100, 999)}"] if "Lookalike" in campanha["mktsettings"].get("target_audience", "") else []
                },
                
                "created_at": criado_em,
                "updated_at": datetime.now(),
                "is_active": is_active,
                "row_guid": str(uuid.uuid4())
            }
            
            # Validação Strict via Pydantic
            adgroup_validado = MarketingAdGroup(**payload).model_dump()
            batch.append(adgroup_validado)
            
            adgroup_counter_id += 1
            
    return batch


if __name__ == "__main__":
    import json
    
    grupos_anuncios = get_marketing_adgroup_seeders()
    print(f"Total de AdGroups simulados vinculados às suas Campanhas: {len(grupos_anuncios)}")
    print("-" * 50)
    
    print("\n--- AMOSTRA DO CRUZAMENTO SEMÂNTICO (INTERESSES/MOCK DE CLIENTES) ---")
    if grupos_anuncios:
        amostra = random.choice(grupos_anuncios)
        print(f"Nome AdGroup: {amostra['ad_group_name']}")
        print(f"Idade Alvo: {amostra['targeting_criteria']['age_range']['min_age']} a {amostra['targeting_criteria']['age_range']['max_age']} anos")
        print(f"Interesses (Vindos do Customer Demographics): {amostra['targeting_criteria']['interests']}")
        print(f"Keywords  (Vindos do Customer Demographics): {amostra['targeting_criteria']['keywords']}")
        print(f"Geo Include: {amostra['targeting_criteria']['geo_location']['include']}")
        
        print("\n--- AMOSTRA PAYLOAD COMPLETO (JSONB JSON) ---")
        print(json.dumps(amostra, indent=2, default=str))
