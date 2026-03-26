import asyncio
import argparse
import sys
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from sqlalchemy import select
from api.database import AsyncSessionLocal
from api.services.warehouse.transaction_history_service import TransactionHistoryService
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema, TraceabilityInfo, IoTSensorData
from api.models.product.product import ProductModel
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.models.warehouse.transactiontype import TransactionTypeModel
from api.models.sales.salesorder import SalesOrderModel

async def seed_transaction_history(count: int):
    """
    Seeder complexo para Histórico de Transações.
    Gera movimentos simulados amarrando produtos e ordens REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar IDs válidos para garantir integridade referencial
        product_ids = (await session.execute(select(ProductModel.product_id))).scalars().all()
        location_ids = (await session.execute(select(WarehouseLocationModel.warehouselocation_id))).scalars().all()
        txn_type_ids = (await session.execute(select(TransactionTypeModel.transactiontype_id))).scalars().all()
        order_ids = (await session.execute(select(SalesOrderModel.salesorder_id))).scalars().all()

        if not product_ids or not location_ids or not txn_type_ids:
            print("⚠️ Tabelas base vazias! Certifique-se de rodar os seeders de Product, Location e TransactionType primeiro.")
            return

        service = TransactionHistoryService(session)
        
        batch_size = 500
        total_created = 0
        
        while total_created < count:
            current_batch_size = min(batch_size, count - total_created)
            batch_data = []
            
            for i in range(current_batch_size):
                qty = random.randint(1, 50) # Reduzido para mais realismo (balanço de estoque)
                cost = Decimal(random.uniform(10.0, 1200.0)).quantize(Decimal("0.0001"))
                
                type_id = random.choice(txn_type_ids)
                
                txn = TransactionHistorySchema(
                    transaction_id=f"TR-{random.randint(100000000, 999999999)}",
                    product_id=random.choice(product_ids),
                    reference_order_id=random.choice(order_ids) if order_ids else None,
                    reference_order_line_id=random.randint(1, 10) if order_ids else None,
                    transaction_date=datetime.now() - timedelta(days=random.randint(0, 90)),
                    transactiontype_id=type_id,
                    quantity=qty,
                    actual_cost=cost,
                    traceability_info=TraceabilityInfo(
                        reason="Movimentação de estoque (Simulação Auditada)",
                        performed_by_user=f"operador_{random.randint(1, 20)}@dataplatform.com",
                        source_warehouselocation_id=random.choice(location_ids),
                        target_warehouselocation_id=random.choice(location_ids) if type_id != 2 else None,
                        quality_check_status=random.choices(["pass", "fail", "hold"], weights=[90, 5, 5])[0],
                        iot_sensor_data=IoTSensorData(
                            temp_at_scan=random.uniform(15.0, 25.0),
                            device_id=f"IOT-SCANNER-{random.randint(100, 999)}"
                        )
                    )
                )
                batch_data.append(txn)
            
            print(f"📦 Injetando batch de {current_batch_size} transações (Integridade Dimensional OK)...")
            
            try:
                await service.process_bulk_transactions(batch_data)
                total_created += current_batch_size
            except Exception as e:
                print(f"❌ Erro no batch: {e}")
                break
        
        try:
            await session.commit()
            print(f"✅ Seeder finalizado! {total_created} registros auditados inseridos.")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao comitar transação final: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Warehouse Transaction History")
    parser.add_argument("--count", type=int, default=100, help="Quantidade de transações a gerar")
    args = parser.parse_args()
    
    asyncio.run(seed_transaction_history(args.count))
