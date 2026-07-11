import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.models.purchasing.purchaseorderdetail import PurchaseOrderDetailModel
from api.models.product.product import Product
from api.schemas.purchasing.purchasing_orderheader import Status
from api.seeder.chaos import apply_purchaseorderdetail_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_po_detail_payloads(headers_reais: List[dict], product_ids: List[int]) -> List[Dict[str, Any]]:
    """Gera payloads de detalhes de pedido com distribuição exata do sub-total."""
    batch = []
    for h in headers_reais:
        h_id = h["purchaseorderheader_id"]
        sub_total = h["sub_total"]
        num_items = random.randint(1, 4)
        current_sum = Decimal("0.0000")
        
        for i in range(1, num_items + 1):
            p_id = random.choice(product_ids)
            if i == num_items:
                item_total = sub_total - current_sum
                if item_total <= 0: break
                order_qty = 1
                unit_price = item_total
            else:
                safe_max = (sub_total - current_sum) / (num_items - i + 1)
                if safe_max < 1.0: continue
                order_qty = random.randint(1, 5)
                unit_price = Decimal(random.uniform(1.0, float(safe_max/order_qty))).quantize(Decimal("0.0001"))
                item_total = order_qty * unit_price
                current_sum += item_total
            
            due_date = h["ship_date"] + timedelta(days=random.randint(2, 5))
            batch.append({
                "purchaseorderdetail_id": f"{h_id}-{i:02d}",
                "purchaseorderheader_id": h_id,
                "product_id": p_id,
                "due_date": due_date,
                "order_qty": order_qty,
                "unit_price": unit_price,
                "received_qty": order_qty if h["status"] == Status.COMPLETED.value else 0,
                "rejected_qty": 0,
                "stocked_qty": order_qty if h["status"] == Status.COMPLETED.value else 0,
                "is_fully_received": h["status"] == Status.COMPLETED.value,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
    return apply_purchaseorderdetail_chaos(batch)

async def seed_purchase_details():
    """Executa a carga de detalhes de pedido consumindo headers e produtos REAIS."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar Headers Reais (Dimensional Integrity)
        result = await session.execute(select(PurchaseOrderHeaderModel.purchaseorderheader_id, PurchaseOrderHeaderModel.sub_total, PurchaseOrderHeaderModel.ship_date, PurchaseOrderHeaderModel.status))
        headers = [{"purchaseorderheader_id": r[0], "sub_total": r[1], "ship_date": r[2], "status": r[3]} for r in result.all()]
        
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not headers or not product_ids:
            print("⚠️ Headers ou Produtos vazios! Rode os seeders base primeiro.")
            return

        print(f"📣 Semeando detalhes para {len(headers)} pedidos...")
        details = generate_po_detail_payloads(headers, product_ids)
        
        try:
            stmt = insert(PurchaseOrderDetailModel).values(details)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(details)} Detalhes de pedido semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear detalhes: {e}")

if __name__ == "__main__":
    asyncio.run(seed_purchase_details())