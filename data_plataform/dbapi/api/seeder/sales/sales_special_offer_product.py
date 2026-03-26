import random
import asyncio
import sys
import os
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.specialoffer import SpecialOfferModel
from api.models.product.product import ProductModel
from api.models.sales.specialofferproduct import SpecialOfferProductModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_special_offer_products(avg_products_per_offer: int = 5):
    """Executa a vinculação de produtos a ofertas especiais com integridade total."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar IDs válidos (Dimensional Integrity)
        offer_ids = (await session.execute(select(SpecialOfferModel.special_offer_id))).scalars().all()
        product_ids = (await session.execute(select(ProductModel.product_id))).scalars().all()
        
        if not offer_ids or not product_ids:
            print("⚠️ Tabelas SpecialOffer ou Product vazias! Rode os seeders base primeiro.")
            return

        print(f"📣 Semeando vínculos entre {len(offer_ids)} ofertas e produtos...")
        
        mappings = []
        seen_pairs = set()
        
        for offer_id in offer_ids:
            # Selecionar uma quantidade aleatória de produtos para cada oferta
            num_products = random.randint(1, avg_products_per_offer * 2)
            selected_products = random.sample(product_ids, min(num_products, len(product_ids)))
            
            for product_id in selected_products:
                pair = (offer_id, product_id)
                if pair not in seen_pairs:
                    mappings.append({
                        "special_offer_id": offer_id,
                        "product_id": product_id
                    })
                    seen_pairs.add(pair)
        
        try:
            # Bulk Insert para performance
            stmt = insert(SpecialOfferProductModel).values(mappings)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(mappings)} Vínculos de oferta/produto semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear vínculos: {e}")

if __name__ == "__main__":
    asyncio.run(seed_special_offer_products())
