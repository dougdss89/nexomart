import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta, date
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.saleschannel import SalesChannelModel
from api.models.sales.specialoffer import SpecialOfferModel
from api.models.person.customer import CustomerModel
from api.models.person.seller import SellerModel
from api.models.product.product import Product
from api.seeder.chaos import apply_sales_order_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_order_items(order_number: str, product_ids: List[int], count: int = 3) -> List[Dict[str, Any]]:
    items = []
    for i in range(1, count + 1):
        p_id = random.choice(product_ids)
        qty = random.randint(1, 5)
        price = Decimal(random.uniform(10.0, 500.0)).quantize(Decimal("0.01"))
        items.append({
            "line_number": f"{order_number}-{i:02d}",
            "line_id": i,
            "product_id": p_id,
            "sku": f"SKU-{p_id:05d}",
            "quantity": qty,
            "unit_price": float(price),
            "seller_id": random.randint(1, 10), # Dummy seller for marketplace logic
            "delivery_info": {
                "status": "pending",
                "estimated_date": (datetime.now() + timedelta(days=5)).date().isoformat()
            },
            "taxes": [
                {"name": "ICMS", "rate": 0.18, "amount": float(price * qty * Decimal("0.18"))}
            ]
        })
    return items

def generate_sales_order_payloads(
    count: int, 
    customer_ids: List[int], 
    channel_ids: List[int], 
    offer_ids: List[int], 
    seller_ids: List[int],
    product_ids: List[int]
) -> List[Dict[str, Any]]:
    batch = []
    for _ in range(count):
        order_num = f"SO-{random.randint(100000000, 999999999)}"
        items = generate_order_items(order_num, product_ids, random.randint(1, 4))
        total_amount = sum(item["unit_price"] * item["quantity"] for item in items)
        
        order_date = datetime.now() - timedelta(days=random.randint(1, 30))
        order_status = random.choices(
            ["open", "finalized", "partial_shipped", "partial_payment", "canceled"],
            weights=[40, 35, 10, 10, 5],
        )[0]

        batch.append({
            "salesorder_id": None, # Auto-increment
            "salesordernumber": order_num,
            "customer_id": random.choice(customer_ids),
            "saleschannel_id": random.choice(channel_ids),
            "specialoffer_id": random.choice(offer_ids),
            "store_id": random.choice(seller_ids),
            "order_date": order_date,
            "order_status": order_status,
            "total_amount": Decimal(total_amount).quantize(Decimal("0.01")),
            "currency_code": "BRL",
            "order_items": items,
            "payment_info": {
                "is_split": False,
                "transactions": [
                    {
                        "method": "pix",
                        "amount": float(total_amount),
                        "status": "paid",
                        "gateway_id": f"GW-{uuid.uuid4().hex[:6].upper()}"
                    }
                ]
            },
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    return apply_sales_order_chaos(batch)

async def seed_sales_orders(count: int = 50):
    """Executa a carga de pedidos de venda com integridade dimensional total."""
    async with AsyncSessionLocal() as session:
        # 1. Buscar Cadastros Base (Dimensional Integrity)
        customer_ids = (await session.execute(select(CustomerModel.customer_id))).scalars().all()
        channel_ids = (await session.execute(select(SalesChannelModel.saleschannel_id))).scalars().all()
        offer_ids = (await session.execute(select(SpecialOfferModel.special_offer_id))).scalars().all()
        seller_ids = (await session.execute(select(SellerModel.seller_id))).scalars().all()
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not all([customer_ids, channel_ids, offer_ids, seller_ids, product_ids]):
            print("⚠️ Tabelas base (Customers, Channels, Offers, Sellers, Products) vazias!")
            return

        print(f"📣 Semeando {count} pedidos de venda (Fact Table)...")
        orders = generate_sales_order_payloads(count, customer_ids, channel_ids, offer_ids, seller_ids, product_ids)
        
        try:
            stmt = insert(SalesOrderModel).values(orders)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(orders)} Pedidos de venda semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear pedidos: {e}")

if __name__ == "__main__":
    asyncio.run(seed_sales_orders())
