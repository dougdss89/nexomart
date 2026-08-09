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
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args, insert_in_batches

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_po_detail_payloads(real_headers: List[dict], product_ids: List[int], chaos_rate: float = 0.05, apply_chaos: bool = True) -> List[Dict[str, Any]]:
    """Generates order detail payloads with an exact distribution of the sub-total."""
    batch = []
    for h in real_headers:
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

    if apply_chaos:
        batch = apply_purchaseorderdetail_chaos(batch, chaos_rate=chaos_rate)

    return batch

async def seed_purchase_details(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the order details load consuming REAL headers and products."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch real Headers that don't have a detail yet (purchaseorderdetail_id is a
        # manual "<header_id>-<line>" PK — reprocessing a header that already has a detail
        # collides on the PK and breaks the entire batch INSERT, not just the duplicate).
        already_detailed = set(
            (await session.execute(select(PurchaseOrderDetailModel.purchaseorderheader_id.distinct()))).scalars().all()
        )
        result = await session.execute(select(PurchaseOrderHeaderModel.purchaseorderheader_id, PurchaseOrderHeaderModel.sub_total, PurchaseOrderHeaderModel.ship_date, PurchaseOrderHeaderModel.status))
        headers = [
            {"purchaseorderheader_id": r[0], "sub_total": r[1], "ship_date": r[2], "status": r[3]}
            for r in result.all() if r[0] not in already_detailed
        ]

        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not headers or not product_ids:
            print("⚠️ No header pending a detail (or products empty)! Run the base seeders first.")
            return

        print(f"📣 Seeding details for {len(headers)} orders | chaos={chaos_rate:.2%}")
        details = generate_po_detail_payloads(headers, product_ids, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await insert_in_batches(session, PurchaseOrderDetailModel, details)
            print(f"✅ {total} order details seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding details: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for purchasing.purchaseorderdetail (fan-out of real headers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_purchase_details(chaos_rate=chaos_rate, apply_chaos=apply_chaos))