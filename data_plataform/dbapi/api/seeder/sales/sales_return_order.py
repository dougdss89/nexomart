import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.returnorder import SalesReturnOrderModel
from api.seeder.chaos import apply_return_order_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_return_orders(count: int = 20):
    """Executa a carga de pedidos de devolução com integridade dimensional total."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar Pedidos Originais para Devolução (Dimensional Integrity)
        stmt = select(
            SalesOrderModel.salesordernumber,
            SalesOrderModel.customer_id,
            SalesOrderModel.store_id,
            SalesOrderModel.order_items
        ).limit(100)
        
        orders = (await session.execute(stmt)).all()
        
        if not orders:
            print("⚠️ Tabela SalesOrder vazia! Sem pedidos para devolver.")
            return

        print(f"📣 Semeando {count} pedidos de devolução (RO- pattern)...")
        returns = []
        
        reasons = [
            "Defeito de fábrica", 
            "Arrependimento de compra", 
            "Produto diferente do anunciado",
            "Embalagem violada",
            "Tamanho incorreto"
        ]
        
        statuses = ["pending", "in_inspection", "accepted", "refunded"]
        
        for _ in range(count):
            orig = random.choice(orders)
            
            # Escolher um item do JSONB para devolver
            items = orig.order_items
            if not items: continue
            item = random.choice(items)
            
            product_id = item.get("product_id")
            qty_to_return = random.randint(1, item.get("quantity", 1))
            unit_price = Decimal(str(item.get("unit_price", 0.0)))
            refund_amt = (unit_price * qty_to_return).quantize(Decimal("0.0001"))
            
            req_date = datetime.now() - timedelta(days=random.randint(1, 15))
            
            returns.append({
                "return_id": f"RO-{random.randint(100000000, 999999999)}",
                "requestorder_code": f"REQ-{random.randint(10000, 99999)}",
                "salesordernumber": orig.salesordernumber,
                "customer_id": orig.customer_id,
                "store_id": orig.store_id,
                "product_id": product_id,
                "quantity": qty_to_return,
                "return_reason": random.choice(reasons),
                "return_status": random.choice(statuses),
                "requested_date": req_date,
                "return_date": req_date + timedelta(days=random.randint(1, 3)),
                "refund_amount": refund_amt,
                "inspection_data": {
                    "inspector_user_id": random.randint(1, 10),
                    "condition": "opened_intact",
                    "is_restockable": True,
                    "technical_notes": "Produto em excelente estado, caixa apenas aberta."
                }
            })
        
        returns = apply_return_order_chaos(returns)

        try:
            stmt = insert(SalesReturnOrderModel).values(returns)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(returns)} Pedidos de devolução semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear devoluções: {e}")

if __name__ == "__main__":
    asyncio.run(seed_return_orders())
