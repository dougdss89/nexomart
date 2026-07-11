import asyncio
import argparse
import sys
import os
import random
from datetime import date, datetime, timedelta
from sqlalchemy import select

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.warehouse.product_inventory_service import ProductInventoryService
from api.schemas.warehouse.warehouse_productinventory import ProductInventorySchema, InventoryAttributes
from api.models.product.product import Product
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.seeder.chaos import apply_inventory_chaos

async def seed_product_inventory(count: int):
    """
    Seeder para Saldos de Estoque com Integridade Dimensional.
    Gera estoque inicial para produtos e locais REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar IDs válidos
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()
        location_ids = (await session.execute(select(WarehouseLocationModel.warehouselocation_id))).scalars().all()

        if not product_ids or not location_ids:
            print("⚠️ Tabelas de Product ou WarehouseLocation vazias! Carga cancelada.")
            return

        service = ProductInventoryService(session)
        
        batch_data = []
        for i in range(count):
            lot = f"LOT-{random.randint(100, 999)}-{random.choice(['A', 'B', 'C'])}"
            mfg_date = date.today() - timedelta(days=random.randint(30, 200))
            exp_date = mfg_date + timedelta(days=random.randint(365, 730))
            
            inv = ProductInventorySchema(
                inventory_id=None, # Auto-increment
                product_id=random.choice(product_ids),
                warehouselocation_id=random.choice(location_ids),
                quantity=random.randint(50, 1000),
                bin_code=f"BIN-{random.choice(['A','B','C'])}-{random.randint(1, 999)}",
                last_restock_date=datetime.now(),
                attributes=InventoryAttributes(
                    lot_number=lot,
                    manufacturing_date=mfg_date,
                    expiration_date=exp_date
                )
            )
            batch_data.append(inv)

        batch_data = apply_inventory_chaos(batch_data)

        print(f"📦 Injetando {len(batch_data)} registros de saldo de estoque (Integridade OK)...")
        
        try:
            await service.restock_inventory(batch_data)
            await session.commit()
            print(f"✅ Seeder finalizado com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro na carga: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Warehouse Product Inventory")
    parser.add_argument("--count", type=int, default=50, help="Quantidade de registros de estoque a gerar")
    args = parser.parse_args()
    
    asyncio.run(seed_product_inventory(args.count))
