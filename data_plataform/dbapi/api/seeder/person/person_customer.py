import argparse
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
from api.seeder.chaos import apply_customer_chaos, apply_false_foreigner_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_customer_payloads(quantidade: int, country_ids: List[int], states_by_country: Dict[int, List[int]]) -> List[Dict[str, Any]]:
    """Gera payloads de clientes vinculados a geografias REAIS (state_province_id sempre pertence ao country_id sorteado)."""
    batch = []
    for _ in range(quantidade):
        sample_data = generate_mock_customer()
        country_id = random.choice(country_ids)
        batch.append({
            "customer_name": sample_data["customer_name"],
            "last_name": sample_data["last_name"],
            "social_number": sample_data["social_number"],
            "birthdate": sample_data["birthdate"],
            "customer_type": sample_data["customer_type"],
            "credit_limit": sample_data["credit_limit"],
            "demographics": sample_data["demographics"],
            "country_id": country_id,
            "state_province_id": random.choice(states_by_country[country_id]),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

    batch = apply_customer_chaos(batch)
    batch = apply_false_foreigner_chaos(batch, states_by_country)
    return batch

async def seed_person_customers(count: int = 100):
    """Executa a carga de clientes consumindo geografias REAIS do banco."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar o mapa de estados por país (Dimensional Integrity: state sempre pertence ao country sorteado)
        estados_result = (await session.execute(select(StateProvinceModel.state_province_id, StateProvinceModel.country_id))).all()
        states_by_country: Dict[int, List[int]] = {}
        for state_id, country_id in estados_result:
            states_by_country.setdefault(country_id, []).append(state_id)

        # Só sorteamos países que já têm pelo menos um estado cadastrado
        country_ids = list(states_by_country.keys())

        if not country_ids:
            print("⚠️ Tabelas de Country ou StateProvince vazias! Rode os seeders de geografia primeiro.")
            return

        print(f"📣 Semeando {count} clientes (Integridade OK)...")
        customers = generate_customer_payloads(count, country_ids, states_by_country)

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

    asyncio.run(seed_person_customers(args.count))