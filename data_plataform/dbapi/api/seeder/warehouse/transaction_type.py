import asyncio
import argparse
import sys
import os
from typing import List

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.warehouse.transaction_type_service import TransactionTypeService
from api.schemas.warehouse.warehouse_transaction_type import TransactionTypeSchema, ActionEffect

async def seed_transaction_types(count: int):
    """
    Seeder para tipos de transação (Master Data).
    """
    async with AsyncSessionLocal() as session:
        service = TransactionTypeService(session)
        
        # 1. Tipos de Transação Primários (Obrigatórios para o negócio conforme AdventureWorks)
        primary_types = [
            TransactionTypeSchema(
                transactiontype_id=1,
                transaction_code="W",
                transaction_name="Work Order",
                description="Transaction relating to a work order.",
                action_effect=ActionEffect.NEUTRAL
            ),
            TransactionTypeSchema(
                transactiontype_id=2,
                transaction_code="S",
                transaction_name="Sales Order",
                description="Transaction relating to a sales order.",
                action_effect=ActionEffect.OUT
            ),
            TransactionTypeSchema(
                transactiontype_id=3,
                transaction_code="P",
                transaction_name="Purchase Order",
                description="Transaction relating to a purchase order.",
                action_effect=ActionEffect.IN
            )
        ]
        
        # Caso queira gerar códigos extras caso count > len(primary_types)
        # Para Master Data, geralmente mantemos apenas os códigos reais.
        to_seed = primary_types[:count] if count < len(primary_types) else primary_types
        
        print(f"🚀 Iniciando carga de {len(to_seed)} tipos de transação no banco...")
        
        try:
            await service.seed_initial_data(to_seed)
            await session.commit()
            print(f"✅ Carga finalizada com sucesso! Total: {len(to_seed)}")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro na carga: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Warehouse Transaction Types")
    parser.add_argument("--count", type=int, default=10, help="Quantidade de tipos a carregar (Master Data)")
    args = parser.parse_args()
    
    # Executa com uv loop
    asyncio.run(seed_transaction_types(args.count))
