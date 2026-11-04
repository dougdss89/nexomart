import asyncio
import os
import random
import sys
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

from api.config.seeder_config import get_ranges
from api.database import AsyncSessionLocal
from api.models.product.product import Product
from api.models.purchasing.purchaseorderdetail import PurchaseOrderDetailModel
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.schemas.purchasing.purchasing_orderheader import Status
from api.schemas.purchasing.purchasing_purchaseorderdetail import PurchaseOrderDetailSchema
from api.seeder.chaos import apply_purchaseorderdetail_chaos
from api.utils.cli_common import add_chaos_args, build_parser, bulk_copy, resolve_chaos_args
from api.utils.quarantine import get_quarantine
from sqlalchemy import func, select

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.purchaseorderdetail"


def generate_po_detail_payloads(
    real_headers: list[dict],
    product_ids: list[int],
    chaos_rate: float = 0.05,
    apply_chaos: bool = True,
) -> list[dict[str, Any]]:
    """Generates order detail payloads with an exact distribution of the sub-total."""
    batch = []
    q = get_quarantine("purchasing.purchaseorderdetail")
    for h in real_headers:
        h_id = h["purchaseorderheader_id"]
        sub_total = h["sub_total"]
        num_items = random.randint(1, 4)
        current_sum = Decimal("0.0000")

        for i in range(1, num_items + 1):
            p_id = random.choice(product_ids)
            if i == num_items:
                item_total = sub_total - current_sum
                if item_total <= 0:
                    break
                order_qty = 1
                unit_price = item_total
            else:
                safe_max = (sub_total - current_sum) / (num_items - i + 1)
                if safe_max < 1.0:
                    continue
                order_qty = random.randint(1, 5)
                unit_price = Decimal(random.uniform(1.0, float(safe_max / order_qty))).quantize(
                    Decimal("0.0001")
                )
                item_total = order_qty * unit_price
                current_sum += item_total

            due_date = h["ship_date"] + timedelta(days=random.randint(2, 5))
            is_received = h["status"] == Status.COMPLETED.value
            payload = {
                "purchaseorderdetail_id": f"{h_id}-{i:02d}",
                "purchaseorderheader_id": h_id,
                "product_id": p_id,
                "due_date": due_date,
                # A received line must carry when it was received (schema Rule 2)
                "last_receipt_date": due_date if is_received else None,
                "order_qty": order_qty,
                "unit_price": unit_price,
                "received_qty": order_qty if is_received else 0,
                "rejected_qty": 0,
                "stocked_qty": order_qty if is_received else 0,
                "is_fully_received": is_received,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": uuid.uuid4(),
            }
            with q.row(payload):
                batch.append(PurchaseOrderDetailSchema(**payload).model_dump())

    if apply_chaos:
        batch = apply_purchaseorderdetail_chaos(batch, chaos_rate=chaos_rate)

    q.finish()
    return batch


async def seed_purchase_details(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the order details load consuming REAL headers and products.

    Root-cause fix (2026-08-17, revised same day): first attempt used an anti-join
    (`LEFT JOIN ... WHERE purchaseorderdetail.purchaseorderheader_id IS NULL`) instead of
    loading both full ID columns into Python — correct, but still measured slow against ~1.4M
    accumulated headers: Postgres has no index that identifies *which* headers lack a detail,
    so a Nested Loop Anti Join still walks header rows roughly in physical/insertion order
    until it accumulates enough misses, i.e. it still scans through most of the *matched* rows
    first. purchaseorderheader_id can't be used as a high-water mark either (it's a random
    "PO-XXXXXXXXX" business ID, not sequential — a later-created header can sort lower than an
    earlier one), so this uses `created_at` instead (indexed by AuditMixin on every table):
    "pending" = headers created after the last detail batch's created_at, a plain indexed
    range scan. Correct as long as a run either fully commits its detail batch or rolls back
    entirely on error (true here), and as long as detail-seeding always runs strictly after
    header-seeding within the same DAG execution (true per the DAG's facts→derived TaskGroup
    ordering) — otherwise the high-water mark could advance past a header whose detail hasn't
    landed yet.
    """
    fan_out_limit = get_ranges(
        schema="purchasing", table="purchasing.purchaseorderheader"
    ).max_count

    async with AsyncSessionLocal() as session:
        watermark = await session.scalar(select(func.max(PurchaseOrderDetailModel.created_at)))
        # purchaseorderdetail_id is a manual "<header_id>-<line>" PK — reprocessing a header
        # that already has a detail collides on the PK and breaks the entire batch INSERT,
        # not just the duplicate, hence bounding by the watermark rather than a plain fetch.
        pending_stmt = (
            select(
                PurchaseOrderHeaderModel.purchaseorderheader_id,
                PurchaseOrderHeaderModel.sub_total,
                PurchaseOrderHeaderModel.ship_date,
                PurchaseOrderHeaderModel.status,
            )
            .order_by(PurchaseOrderHeaderModel.created_at)
            .limit(fan_out_limit)
        )
        if watermark is not None:
            pending_stmt = pending_stmt.where(PurchaseOrderHeaderModel.created_at > watermark)
        result = await session.execute(pending_stmt)
        headers = [
            {"purchaseorderheader_id": r[0], "sub_total": r[1], "ship_date": r[2], "status": r[3]}
            for r in result.all()
        ]

        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not headers or not product_ids:
            print("⚠️ No header pending a detail (or products empty)! Run the base seeders first.")
            return

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) scales with total accumulated rows, not this run's work — same class
            # of bug as the fan-out fixes above, so only pay for it when absolute_max is
            # actually set (currently None for this table, making the check a no-op).
            existing_count = (
                await session.scalar(
                    select(func.count(PurchaseOrderDetailModel.purchaseorderdetail_id))
                )
                or 0
            )
            if existing_count >= ranges.absolute_max:
                print(
                    f"⚠️ Absolute purchase order detail limit reached ({existing_count:,}/{ranges.absolute_max:,})."
                )
                return
            # Fan-out seeder: each header yields 1-4 detail lines (avg ~2.5), so headers
            # are capped conservatively (÷3) to avoid overshooting absolute_max.
            max_headers = max(1, (ranges.absolute_max - existing_count) // 3)
            headers = headers[:max_headers]

        existing_display = f"{existing_count:,}" if existing_count is not None else "n/a"
        print(
            f"📣 Seeding details for {len(headers)} orders (existing details: {existing_display}) | chaos={chaos_rate:.2%}"
        )
        details = generate_po_detail_payloads(
            headers, product_ids, chaos_rate=chaos_rate, apply_chaos=apply_chaos
        )

        try:
            total = await bulk_copy(session, PurchaseOrderDetailModel, details)
            print(f"✅ {total} order details seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding details: {e}")
            raise RuntimeError(f"Purchase order detail seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for purchasing.purchaseorderdetail (fan-out of real headers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_purchase_details(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
