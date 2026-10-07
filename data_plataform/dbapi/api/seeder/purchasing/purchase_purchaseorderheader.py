import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, func
from api.database import AsyncSessionLocal
from api.schemas.purchasing.purchasing_orderheader import Status, AuditAction, PurchaseOrderHeader
from api.models.purchasing.vendor import PurchaseVendorModel
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.seeder.chaos import apply_purchaseorderheader_chaos
from api.utils.cli_common import build_parser, add_volume_args, resolve_volume_args, generate_unique_business_ids, bulk_copy
from api.utils.quarantine import get_quarantine
from api.config.seeder_config import get_ranges

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.purchaseorderheader"

# Widened 2026-09-05 (was 1-365 days back) so order_date spans multiple years.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

def generate_po_header_payloads(vendor_ids: List[int], employee_ids: List[int], header_ids: List[str]) -> List[Dict[str, Any]]:
    """Generates purchase order header payloads linked to REAL entities.

    `header_ids` must be pre-generated, collision-checked IDs (see generate_unique_business_ids)
    — one per record.
    """
    batch = []
    q = get_quarantine("purchasing.purchaseorderheader")
    for header_id in header_ids:
        order_date = datetime.now() - timedelta(days=random.randint(1, (datetime.now() - EARLIEST_TRANSACTION_DATE).days))
        status = random.choice(list(Status))

        sub_total = Decimal(random.uniform(1000.0, 50000.0)).quantize(Decimal("0.0001"))
        tax_amt = (sub_total * Decimal("0.12")).quantize(Decimal("0.0001"))
        freight = Decimal(random.uniform(50.0, 500.0)).quantize(Decimal("0.0001"))
        total_due = sub_total + tax_amt + freight
        employee_id = random.choice(employee_ids)
        ship_date = order_date + timedelta(days=random.randint(1, 10))

        payload = {
            "purchaseorderheader_id": header_id,
            "vendor_id": random.choice(vendor_ids),
            "employee_id": employee_id,
            "order_date": order_date,
            "ship_date": ship_date,
            # schema rule: a completed order must carry its finish date
            "orderfinished_date": ship_date + timedelta(days=random.randint(1, 10)) if status == Status.COMPLETED else None,
            "sub_total": sub_total,
            "tax_amt": tax_amt,
            "freight": freight,
            "total_due": total_due,
            "status": status.value,
            "audit_trail": [{
                "date": order_date.isoformat(),
                "action": AuditAction.CREATED.value,
                "employee_id": str(employee_id),
                "comments": "System generated",
            }],
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        }
        with q.row(payload):
            header_obj = PurchaseOrderHeader(**payload)
            validated_header = header_obj.model_dump()
            # audit_trail entries carry a datetime (action_date) — JSON-safe dump for the JSONB column.
            # by_alias=True keeps the key as "date" (its alias): the active expression index
            # ix_audit_trail_date (models/.../purchaseorderheader.py) indexes audit_trail[0]->>'date',
            # so dumping the field name "action_date" instead would index NULL on every row.
            validated_header["audit_trail"] = [entry.model_dump(mode="json", by_alias=True) for entry in header_obj.audit_trail]
            batch.append(validated_header)
    q.finish()
    return batch

async def seed_purchase_orders(count: int = 50, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the purchase order load consuming REAL vendors and employees."""
    async with AsyncSessionLocal() as session:
        vendor_ids = (await session.execute(select(PurchaseVendorModel.vendor_id))).scalars().all()
        employee_ids = (await session.execute(select(HumanResourcesEmployeeModel.employee_id))).scalars().all()

        if not vendor_ids or not employee_ids:
            print("⚠️ Vendor or Employee tables are empty! Run the base seeders first.")
            return

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) scales with total accumulated history, not this run's work — only pay
            # for it when absolute_max is actually set (currently None for this table, making
            # the check a no-op). Same fix as purchase_purchaseorderdetail.py/person_customer.py.
            existing_count = await session.scalar(select(func.count(PurchaseOrderHeaderModel.purchaseorderheader_id))) or 0
            if existing_count >= ranges.absolute_max:
                print(f"⚠️ Absolute purchase order limit reached ({existing_count:,}/{ranges.absolute_max:,}).")
                return
            count = min(count, ranges.absolute_max - existing_count)

        final_total = f"{existing_count + count:,}" if existing_count is not None else "n/a"
        print(f"📣 Seeding {count:,} purchase orders (final total: {final_total}) | chaos={chaos_rate:.2%}")

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            header_ids = await generate_unique_business_ids(
                session, PurchaseOrderHeaderModel.purchaseorderheader_id,
                prefix="PO", count=current_batch_size,
            )
            pos = generate_po_header_payloads(vendor_ids, employee_ids, header_ids)

            if apply_chaos:
                pos = apply_purchaseorderheader_chaos(pos, chaos_rate=chaos_rate)

            try:
                await bulk_copy(session, PurchaseOrderHeaderModel, pos)
                total_inserted += len(pos)
                print(f"   ⏳ Progress: {total_inserted:,}/{count:,}")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding orders: {e}")
                raise RuntimeError(f"Purchase order seeding aborted ({total_inserted:,}/{count:,} inserted): {e}") from e

        print(f"✅ {total_inserted:,} purchase orders seeded successfully!")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_purchase_orders(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))