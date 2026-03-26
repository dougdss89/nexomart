import asyncio
import argparse
import sys
import os

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.sales.sales_channel_service import SalesChannelService
from api.schemas.sales.sales_channel import SalesChannelSchema
from api.samples.sales_channel import SALES_CHANNEL_SAMPLES

async def seed_sales_channels(count: int):
    """
    Seeder para Canais de Venda.
    Importa dados da pasta de samples e utiliza o Service para carga massiva.
    """
    async with AsyncSessionLocal() as session:
        service = SalesChannelService(session)
        
        # Pega do sample, limitando pelo count se necessário
        samples_to_use = SALES_CHANNEL_SAMPLES[:count]
        
        print(f"💰 Preparando carga de {len(samples_to_use)} canais de venda...")
        
        # Converte dicionários de sample em schemas Pydantic para validação
        payload = [SalesChannelSchema(**item) for item in samples_to_use]
        
        try:
            await service.seed_initial_channels(payload)
            await session.commit()
            print(f"✅ Seeder finalizado com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro na carga: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Sales Channels")
    parser.add_argument("--count", type=int, default=5, help="Quantidade de canais a carregar")
    args = parser.parse_args()
    
    asyncio.run(seed_sales_channels(args.count))
