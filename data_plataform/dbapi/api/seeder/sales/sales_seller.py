import asyncio
import random
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.person.seller import SellerModel
from api.models.person.countryregion import CountryRegionModel

# Amostras
from api.samples.sales_seller import (
    SELLER_NAMES, TRADING_NAMES, BANK_CODES, BANK_NAMES, 
    SELLER_TYPES, ADDRESS_STREETS, CITIES, STATES
)

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_sellers(count: int = 30):
    """Executa a carga de vendedores com integridade dimensional total."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar IDs válidos (Dimensional Integrity)
        country_ids = (await session.execute(select(CountryRegionModel.country_id))).scalars().all()
        
        if not country_ids:
            print("⚠️ Tabela CountryRegion vazia! Rode o seeder de países primeiro.")
            return

        print(f"📣 Semeando {count} vendedores (Person.Seller)...")
        sellers = []
        
        for i in range(count):
            name = SELLER_NAMES[i % len(SELLER_NAMES)]
            trading = TRADING_NAMES[i % len(TRADING_NAMES)]
            
            # Gerar social_number realista (CNPJ ou CPF simulado)
            s_type = random.choice(SELLER_TYPES)
            if s_type == "business":
                social = f"{random.randint(10, 99)}.{random.randint(100, 999)}.{random.randint(100, 999)}/0001-{random.randint(10, 99)}"
            else:
                social = f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
            
            bank_idx = random.randint(0, len(BANK_CODES)-1)
            city_idx = random.randint(0, len(CITIES)-1)
            
            opened_date = datetime.now() - timedelta(days=random.randint(365, 3650))
            
            sellers.append({
                "seller_name": name,
                "last_name": "Co. & Partners" if s_type == "business" else name.split()[-1],
                "social_number": social,
                "opened_date": opened_date,
                "commission_rate": Decimal(random.uniform(0.05, 0.20)).quantize(Decimal("0.0001")),
                "country_id": random.choice(country_ids),
                "demographics": {
                    "information": {
                        "sellertype": s_type,
                        "trading_name": trading,
                        "support_email": f"support@{trading.lower()}.com.br"
                    },
                    "bank_account": {
                        "bank_code": BANK_CODES[bank_idx],
                        "bank_name": BANK_NAMES[bank_idx],
                        "agency": f"{random.randint(1000, 9999)}",
                        "account_number": f"{random.randint(10000, 99999)}-{random.randint(0, 9)}",
                        "account_type": "checking"
                    },
                    "address": [
                        {
                            "address_type": "office",
                            "address_line": f"{random.choice(ADDRESS_STREETS)}, {random.randint(1, 2000)}",
                            "postal_code": f"{random.randint(10000, 99999)}-{random.randint(0, 999)}",
                            "city": CITIES[city_idx],
                            "state_name": STATES[city_idx],
                            "country": "Brasil"
                        }
                    ],
                    "metrics": {
                        "total_items_sold": random.randint(100, 10000),
                        "count_orders": random.randint(50, 5000),
                        "date_first_sale": (opened_date + timedelta(days=30)).date().isoformat(),
                        "date_last_sale": date.today().isoformat()
                    }
                }
            })
            
        try:
            stmt = insert(SellerModel).values(sellers)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(sellers)} Vendedores semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear vendedores: {e}")

if __name__ == "__main__":
    asyncio.run(seed_sellers())
