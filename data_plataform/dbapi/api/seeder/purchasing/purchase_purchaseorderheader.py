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
from api.schemas.purchasing.purchasing_orderheader import Status, AuditAction, PurchaseOrderHeader
from api.models.purchasing.vendor import PurchaseVendorModel
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.purchasing.orderheader import PurchaseOrderHeaderModel

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_po_header_payloads(vendor_ids: List[int], employee_ids: List[int], num_records: int = 50) -> List[Dict[str, Any]]:
    """Gera payloads de cabeçalhos de pedido de compra vinculados a entidades REAIS."""
    batch = []
    for i in range(1, num_records + 1):
        order_date = datetime.now() - timedelta(days=random.randint(1, 365))
        status = random.choice(list(Status))
        
        sub_total = Decimal(random.uniform(1000.0, 50000.0)).quantize(Decimal("0.0001"))
        tax_amt = (sub_total * Decimal("0.12")).quantize(Decimal("0.0001"))
        freight = Decimal(random.uniform(50.0, 500.0)).quantize(Decimal("0.0001"))
        total_due = sub_total + tax_amt + freight

        batch.append({
            "purchaseorderheader_id": f"PO-{random.randint(100000000, 999999999)}",
            "vendor_id": random.choice(vendor_ids),
            "employee_id": random.choice(employee_ids),
            "order_date": order_date,
            "ship_date": order_date + timedelta(days=random.randint(1, 10)),
            "sub_total": sub_total,
            "tax_amt": tax_amt,
            "freight": freight,
            "total_due": total_due,
            "status": status.value,
            "audit_trail": [{"date": order_date.isoformat(), "action": AuditAction.CREATED.value, "comments": "System generated"}],
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    return batch

async def seed_purchase_orders(count: int = 50):
    """Executa a carga de pedidos de compra consumindo fornecedores e funcionários REAIS."""
    async with AsyncSessionLocal() as session:
        vendor_ids = (await session.execute(select(PurchaseVendorModel.vendor_id))).scalars().all()
        employee_ids = (await session.execute(select(HumanResourcesEmployeeModel.employee_id))).scalars().all()

        if not vendor_ids or not employee_ids:
            print("⚠️ Tabelas de Vendor ou Employee vazias! Rode os seeders base primeiro.")
            return

        print(f"📣 Semeando {count} pedidos de compra...")
        pos = generate_po_header_payloads(vendor_ids, employee_ids, count)
        
        try:
            stmt = insert(PurchaseOrderHeaderModel).values(pos)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(pos)} Pedidos de compra semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear pedidos: {e}")

if __name__ == "__main__":
    asyncio.run(seed_purchase_orders())