import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert, func
from api.database import AsyncSessionLocal

# Models
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.returnorder import SalesReturnOrderModel
from api.seeder.chaos import apply_return_order_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args, generate_unique_business_ids
from api.config.seeder_config import get_ranges

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.sales_return_order"

# Widened 2026-09-05 (was 1-15 days back) to match the wider order_date range in
# sales_order.py — see EARLIEST_TRANSACTION_DATE there.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

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

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) scales with total accumulated history, not this run's work — only pay
            # for it when absolute_max is actually set (currently None for this table, making
            # the check a no-op). Same fix as purchase_purchaseorderdetail.py/person_customer.py.
            existing_count = await session.scalar(select(func.count(SalesReturnOrderModel.return_id))) or 0
            if existing_count >= ranges.absolute_max:
                print(f"⚠️ Absolute return order limit reached ({existing_count:,}/{ranges.absolute_max:,}).")
                return
            count = min(count, ranges.absolute_max - existing_count)

        final_total = f"{existing_count + count:,}" if existing_count is not None else "n/a"
        print(f"📣 Seeding {count:,} return orders (final total: {final_total}) | chaos={chaos_rate:.2%}")

        reasons = [
            "Defeito de fábrica",
            "Arrependimento de compra",
            "Produto diferente do anunciado",
            "Embalagem violada",
            "Tamanho incorreto"
        ]

        statuses = ["pending", "in_inspection", "accepted", "refunded"]

        def generate_return_batch(return_ids: List[str]) -> List[Dict[str, Any]]:
            batch = []
            for return_id in return_ids:
                orig = random.choice(orders)

                # Pick an item from the JSONB to return
                items = orig.order_items
                if not items: continue
                item = random.choice(items)

                product_id = item.get("product_id")
                qty_to_return = random.randint(1, item.get("quantity", 1))
                unit_price = Decimal(str(item.get("unit_price", 0.0)))
                refund_amt = (unit_price * qty_to_return).quantize(Decimal("0.0001"))

                req_date = datetime.now() - timedelta(days=random.randint(1, (datetime.now() - EARLIEST_TRANSACTION_DATE).days))

                batch.append({
                    "return_id": return_id,
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
            return_ids = await generate_unique_business_ids(
                session, SalesReturnOrderModel.return_id,
                prefix="RO", count=current_batch_size,
            )
            returns = generate_return_batch(return_ids)

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
                raise RuntimeError(f"Return order seeding aborted ({total_inserted:,}/{count:,} inserted): {e}") from e

        print(f"✅ {total_inserted:,} return orders seeded successfully!")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_return_orders(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
