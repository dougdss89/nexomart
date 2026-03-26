import asyncio
import sys
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.samples.state_province import ESTADOS_OPERACAO
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_state_payloads(country_map: Dict[str, int]) -> List[Dict[str, Any]]:
    """Gera a carga de estados vinculados aos países do banco."""
    batch = []
    data_setup = datetime.now()
    for country_code, lista_estados in ESTADOS_OPERACAO.items():
        current_country_id = country_map.get(country_code)
        if not current_country_id:
            continue
        for state_code, state_name in lista_estados:
            batch.append({
                "state_province_id": None, # Auto-increment
                "state_province_name": state_name,
                "state_province_code": state_code,
                "country_id": current_country_id,
                "created_at": data_setup,
                "updated_at": data_setup,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
    return batch

async def seed_person_states():
    """Executa a carga de estados vinculados aos países REAIS do banco."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar mapa de códigos de países para IDs (Dimensional Integrity)
        result = await session.execute(select(CountryRegionModel.country_id, CountryRegionModel.country_code))
        country_map = {row[1]: row[0] for row in result.all()}

        if not country_map:
            print("⚠️ Nenhum país encontrado! Rode o seeder de PersonCountry primeiro.")
            return

        print(f"📣 Semeando estados para {len(country_map)} países ativos...")
        states = generate_state_payloads(country_map)
        
        try:
            stmt = insert(StateProvinceModel).values(states)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(states)} estados semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear estados: {e}")

if __name__ == "__main__":
    asyncio.run(seed_person_states())
