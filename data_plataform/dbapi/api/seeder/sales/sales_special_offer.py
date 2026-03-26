import asyncio
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.specialoffer import SpecialOfferModel

# Amostras
from api.samples.sales_special_offer import SPECIAL_OFFERS

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_special_offers():
    """Executa a carga de ofertas especiais (Master Data)."""
    async with AsyncSessionLocal() as session:
        print(f"📣 Semeando {len(SPECIAL_OFFERS)} ofertas especiais...")
        
        offers = []
        now = datetime.now()
        
        for item in SPECIAL_OFFERS:
            offers.append({
                **item,
                "start_date": now - timedelta(days=random.randint(1, 10)),
                "end_date": now + timedelta(days=random.randint(30, 90)),
                "is_active": True
            })
            
        try:
            # Bulk Insert
            stmt = insert(SpecialOfferModel).values(offers)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(offers)} Ofertas especiais semeadas com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear ofertas: {e}")

if __name__ == "__main__":
    import random # Import local para o helper de data
    asyncio.run(seed_special_offers())
