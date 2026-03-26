import asyncio
import sys
import os
import uuid
import random
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.samples.customer import generate_mock_customer
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel
from api.models.person.customer import CustomerModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_customer_payloads(quantidade: int, country_ids: List[int], state_ids: List[int]) -> List[Dict[str, Any]]:
    """Gera payloads de clientes vinculados a geografias REAIS."""
    batch = []
    for _ in range(quantidade):
        sample_data = generate_mock_customer()
        batch.append({
            "customer_name": sample_data["customer_name"],
            "last_name": sample_data["last_name"],
            "social_number": sample_data["social_number"],
            "birthdate": sample_data["birthdate"],
            "customer_type": sample_data["customer_type"],
            "credit_limit": sample_data["credit_limit"],
            "demographics": sample_data["demographics"],
            "country_id": random.choice(country_ids),
            "state_province_id": random.choice(state_ids),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    return batch

async def seed_person_customers(count: int = 100):
    """Executa a carga de clientes consumindo geografias REAIS do banco."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar IDs válidos (Dimensional Integrity)
        country_ids = (await session.execute(select(CountryRegionModel.country_id))).scalars().all()
        state_ids = (await session.execute(select(StateProvinceModel.state_province_id))).scalars().all()

        if not country_ids or not state_ids:
            print("⚠️ Tabelas de Country ou StateProvince vazias! Rode os seeders de geografia primeiro.")
            return

        print(f"📣 Semeando {count} clientes (Integridade OK)...")
        customers = generate_customer_payloads(count, country_ids, state_ids)
        
        try:
            stmt = insert(CustomerModel).values(customers)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(customers)} clientes semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear clientes: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Person Customers")
    parser.add_argument("--count", type=int, default=100, help="Quantidade de clientes a gerar")
    args = parser.parse_args()
    
    import argparse
    asyncio.run(seed_person_customers(args.count))