import uuid
from datetime import datetime
from api.schemas.marketing.marketing_channel import MarketingChannel, ChannelName, PlatformName

def get_marketing_channel_seeders() -> list[dict]:
    """
    Gera as combinações únicas (Dimensão Larga / Star Schema) de Canal e Plataforma.
    Mapeia logicamente e exaustivamente as formas como uma campanha ou venda pode ser atribuída.
    """
    batch = []
    
    # Combinações lógicas válidas (Source of Truth do DW)
    combinacoes = [
        (ChannelName.PAID_SEARCH, PlatformName.GOOGLE_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.META_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.TIKTOK),
        (ChannelName.SOCIAL_MEDIA, PlatformName.LINKEDIN_ADS),
        (ChannelName.EMAIL, PlatformName.MAILCHIMP),
        (ChannelName.ORGANIC, PlatformName.GA4), # Buscas orgânicas rastreadas
        (ChannelName.ORGANIC, PlatformName.NONE),
        (ChannelName.DIRECT, PlatformName.GA4),
        (ChannelName.DIRECT, PlatformName.NONE),
        (ChannelName.AFFILIATE, PlatformName.NONE),
        (ChannelName.OTHER, PlatformName.NONE),
    ]
    
    data_hoje = datetime.now()
    
    for indice, (canal, plataforma) in enumerate(combinacoes, start=1):
        
        payload = {
            "channel_id": indice,
            "channel_name": canal.value,
            "platform_name": plataforma.value,
            "created_at": data_hoje,
            "updated_at": data_hoje,
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        }
        
        # Validação estrita (Pydantic barra nomes inválidos fora do Enum)
        canal_validado = MarketingChannel(**payload).model_dump()
        batch.append(canal_validado)
        
    return batch

if __name__ == "__main__":
    import json
    canais = get_marketing_channel_seeders()
    print(f"Total de combinações geradas p/ DimTabela (Channels): {len(canais)}\n")
    print("-" * 50)
    for c in canais:
        print(f"ID {c['channel_id']:02d} | Canal: {c['channel_name']:<15} | Plataforma: {c['platform_name']}")
    
    print("-" * 50)
    print("\n--- AMOSTRA DE PAYLOAD (JSON) ---")
    print(json.dumps(canais[0], indent=2, default=str))
