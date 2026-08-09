import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Models
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.returnorder import SalesReturnOrderModel
from api.seeder.chaos import apply_return_order_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.sales_return_order"

async def seed_return_orders(count: int = 20, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the return order load with full dimensional integrity."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch original orders to return (Dimensional Integrity)
        stmt = select(
            SalesOrderModel.salesordernumber,
            SalesOrderModel.customer_id,
            SalesOrderModel.store_id,
            SalesOrderModel.order_items
        ).limit(100)

        orders = (await session.execute(stmt)).all()

        if not orders:
            print("⚠️ SalesOrder table is empty! No orders to return.")
            return

        print(f"📣 Seeding {count:,} return orders (RO- pattern) | chaos={chaos_rate:.2%}")

        reasons = [
            "Defeito de fábrica",
            "Arrependimento de compra",
            "Produto diferente do anunciado",
            "Embalagem violada",
            "Tamanho incorreto"
        ]

        statuses = ["pending", "in_inspection", "accepted", "refunded"]

        def generate_return_batch(n: int) -> List[Dict[str, Any]]:
            batch = []
            for _ in range(n):
                orig = random.choice(orders)

                # Pick an item from the JSONB to return
                items = orig.order_items
                if not items: continue
                item = random.choice(items)

                product_id = item.get("product_id")
                qty_to_return = random.randint(1, item.get("quantity", 1))
                unit_price = Decimal(str(item.get("unit_price", 0.0)))
                refund_amt = (unit_price * qty_to_return).quantize(Decimal("0.0001"))

                req_date = datetime.now() - timedelta(days=random.randint(1, 15))

                batch.append({
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
            return batch

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            returns = generate_return_batch(current_batch_size)

            if apply_chaos:
                returns = apply_return_order_chaos(returns, chaos_rate=chaos_rate)

            try:
                stmt = insert(SalesReturnOrderModel).values(returns)
                await session.execute(stmt)
                await session.commit()
                total_inserted += len(returns)
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding returns: {e}")
                break

        print(f"✅ {total_inserted:,} return orders seeded successfully!")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_return_orders(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
