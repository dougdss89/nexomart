import asyncio
import sys
import os
import uuid 
import pycountry
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.samples.country import PAISES_OPERACAO
from api.models.person.country_region import CountryRegionModel
from api.seeder.chaos import apply_geography_code_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_country_seeders() -> List[Dict[str, Any]]:
    """Gera a carga de países regionais."""
    batch = []
    data_setup = datetime.now() - timedelta(days=730)
    for index, (code, region) in enumerate(PAISES_OPERACAO, start=1):
        country = pycountry.countries.get(alpha_2=code)
        batch.append({
            "country_id": None, # Auto-increment
            "country_code": code,
            "country_region": region,
            "country_name": country.name if country else code,
            "created_at": data_setup,
            "updated_at": data_setup,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    return apply_geography_code_chaos(batch, code_field="country_code")

async def seed_person_countries():
    """Executa a carga de países no banco."""
    async with AsyncSessionLocal() as session:
        countries = get_country_seeders()
        print(f"📣 Semeando {len(countries)} países...")
        try:
            stmt = insert(CountryRegionModel).values(countries)
            await session.execute(stmt)
            await session.commit()
            print("✅ Países semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear países: {e}")

if __name__ == "__main__":
    asyncio.run(seed_person_countries())
