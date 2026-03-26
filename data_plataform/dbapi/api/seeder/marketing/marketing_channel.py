import asyncio
import sys
import os
import uuid
from datetime import datetime
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.schemas.marketing.marketing_channel import MarketingChannel, ChannelName, PlatformName
from api.models.marketing.marketing_channel import MarketingChannelModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_marketing_channel_seeders() -> list[dict]:
    """
    Gera as combinações únicas (Dimensão Larga) de Canal e Plataforma.
    """
    batch = []
    combinacoes = [
        (ChannelName.PAID_SEARCH, PlatformName.GOOGLE_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.META_ADS),
        (ChannelName.SOCIAL_MEDIA, PlatformName.TIKTOK),
        (ChannelName.SOCIAL_MEDIA, PlatformName.LINKEDIN_ADS),
        (ChannelName.EMAIL, PlatformName.MAILCHIMP),
        (ChannelName.ORGANIC, PlatformName.GA4),
        (ChannelName.ORGANIC, PlatformName.NONE),
        (ChannelName.DIRECT, PlatformName.GA4),
        (ChannelName.DIRECT, PlatformName.NONE),
        (ChannelName.AFFILIATE, PlatformName.NONE),
        (ChannelName.OTHER, PlatformName.NONE),
    ]
    data_hoje = datetime.now()
    for indice, (canal, plataforma) in enumerate(combinacoes, start=1):
        payload = {
            "channel_id": None, # Auto-increment
            "channel_name": canal.value,
            "platform_name": plataforma.value,
            "created_at": data_hoje,
            "updated_at": data_hoje,
            "is_active": True,
            "row_guid": uuid.uuid4()
        }
        canal_validado = MarketingChannel(**payload).model_dump()
        batch.append(canal_validado)
    return batch

async def seed_marketing_channels():
    """
    Executa a carga dos canais de marketing no banco de dados.
    """
    async with AsyncSessionLocal() as session:
        channels = get_marketing_channel_seeders()
        print(f"📣 Semeando {len(channels)} canais de marketing...")
        try:
            stmt = insert(MarketingChannelModel).values(channels)
            await session.execute(stmt)
            await session.commit()
            print("✅ Canais semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear canais: {e}")

if __name__ == "__main__":
    asyncio.run(seed_marketing_channels())
