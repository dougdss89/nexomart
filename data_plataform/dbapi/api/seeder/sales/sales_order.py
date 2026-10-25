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
from api.models.person.customer import CustomerModel
from api.models.person.seller import SellerModel
from api.models.product.product import Product
from api.models.sales.saleschannel import SalesChannelModel

# Modelos
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.specialoffer import SpecialOfferModel
from api.schemas.sales.sales_order import SalesOrderSchema
from api.seeder.chaos import apply_sales_order_chaos
from api.utils.cli_common import (
    add_volume_args,
    build_parser,
    bulk_copy,
    generate_unique_business_ids,
    resolve_volume_args,
    sample_fk_ids,
)
from api.utils.quarantine import get_quarantine
from sqlalchemy import func, select

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"

# Widened 2026-09-05 (was 1-30 days back) so order_date spans multiple years instead
# of only the last month, matching purchaseorderheader/transactionhistory/etc.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)


def generate_order_items(
    order_number: str, product_ids: list[int], count: int = 3
) -> list[dict[str, Any]]:
    items = []
    for i in range(1, count + 1):
        p_id = random.choice(product_ids)
        qty = random.randint(1, 5)
        price = Decimal(random.uniform(10.0, 500.0)).quantize(Decimal("0.01"))
        items.append(
            {
                "line_number": f"{order_number}-{i:02d}",
                "line_id": i,
                "product_id": p_id,
                "sku": f"SKU-{p_id:05d}",
                "quantity": qty,
                "unit_price": float(price),
                "seller_id": random.randint(1, 10),  # Dummy seller for marketplace logic
                "delivery_info": {
                    "status": "pending",
                    "estimated_date": (datetime.now() + timedelta(days=5)).date().isoformat(),
                },
                "taxes": [
                    {"name": "ICMS", "rate": 0.18, "amount": float(price * qty * Decimal("0.18"))}
                ],
            }
        )
    return items


def generate_sales_order_payloads(
    order_numbers: list[str],
    customer_ids: list[int],
    channel_ids: list[int],
    offer_ids: list[int],
    seller_ids: list[int],
    product_ids: list[int],
) -> list[dict[str, Any]]:
    """`order_numbers` must be pre-generated, collision-checked IDs (see generate_unique_business_ids)."""
    batch = []
    q = get_quarantine("sales.salesorder")
    for order_num in order_numbers:
        items = generate_order_items(order_num, product_ids, random.randint(1, 4))
        total_amount = sum(
            Decimal(str(item["unit_price"])) * item["quantity"] for item in items
        ).quantize(Decimal("0.01"))

        order_date = datetime.now() - timedelta(
            days=random.randint(1, (datetime.now() - EARLIEST_TRANSACTION_DATE).days)
        )
        order_status = random.choices(
            ["open", "finalized", "partial_shipped", "partial_payment", "canceled"],
            weights=[40, 35, 10, 10, 5],
        )[0]

        payload = {
            "salesorder_id": 1,  # placeholder for Pydantic validation (gt=0); removed before insert
            "salesordernumber": order_num,
            "customer_id": random.choice(customer_ids),
            "saleschannel_id": random.choice(channel_ids),
            "specialoffer_id": random.choice(offer_ids),
            "store_id": random.choice(seller_ids),
            "order_date": order_date,
            "order_status": order_status,
            "total_amount": total_amount,
            "currency_code": "BRL",
            "order_items": items,
            "payment_info": {
                "is_split": False,
                "transactions": [
                    {
                        "method": "pix",
                        "amount": float(total_amount),
                        "status": "paid",
                        "gateway_id": f"GW-{uuid.uuid4().hex[:6].upper()}",
                    }
                ],
            },
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4(),
        }
        with q.row(payload):
            order_obj = SalesOrderSchema(**payload)
            validated_order = order_obj.model_dump()
            # order_items/payment_info carry Decimal fields — JSON-safe dump for the JSONB columns
            validated_order["order_items"] = [
                item.model_dump(mode="json") for item in order_obj.order_items
            ]
            validated_order["payment_info"] = order_obj.payment_info.model_dump(mode="json")
            validated_order.pop("salesorder_id", None)  # let the database's autoincrement take over
            batch.append(validated_order)
    q.finish()
    return batch


async def seed_sales_orders(
    count: int = 50, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True
):
    """Runs the sales order load with full dimensional integrity."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch base records (Dimensional Integrity). customer_ids is sampled server-side
        # (2026-08-17 fix) instead of loading the full column — person.customer accumulates
        # unboundedly across runs (millions of rows after a handful of DAG executions), and
        # loading it all into Python on every run just to random.choice() from it doesn't
        # scale for a table meant to run every 10-15 minutes. The other pools here (channels,
        # offers, sellers, products) stay small/bounded, so a full fetch is still fine.
        customer_ids = await sample_fk_ids(session, CustomerModel.customer_id)
        channel_ids = (
            (await session.execute(select(SalesChannelModel.saleschannel_id))).scalars().all()
        )
        offer_ids = (
            (await session.execute(select(SpecialOfferModel.special_offer_id))).scalars().all()
        )
        seller_ids = (await session.execute(select(SellerModel.seller_id))).scalars().all()
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not all([customer_ids, channel_ids, offer_ids, seller_ids, product_ids]):
            print("⚠️ Base tables (Customers, Channels, Offers, Sellers, Products) are empty!")
            return

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) scales with total accumulated history, not this run's work — only pay
            # for it when absolute_max is actually set (currently None for this table, making
            # the check a no-op). Same fix as purchase_purchaseorderdetail.py/person_customer.py.
            existing_count = (
                await session.scalar(select(func.count(SalesOrderModel.salesorder_id))) or 0
            )
            if existing_count >= ranges.absolute_max:
                print(
                    f"⚠️ Absolute sales order limit reached ({existing_count:,}/{ranges.absolute_max:,})."
                )
                return
            count = min(count, ranges.absolute_max - existing_count)

        final_total = f"{existing_count + count:,}" if existing_count is not None else "n/a"
        print(
            f"📣 Seeding {count:,} sales orders (final total: {final_total}) | batch={batch_size:,} | chaos={chaos_rate:.2%}"
        )

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            order_numbers = await generate_unique_business_ids(
                session,
                SalesOrderModel.salesordernumber,
                prefix="SO",
                count=current_batch_size,
            )
            orders = generate_sales_order_payloads(
                order_numbers, customer_ids, channel_ids, offer_ids, seller_ids, product_ids
            )

            if apply_chaos:
                orders = apply_sales_order_chaos(orders, chaos_rate=chaos_rate)

            try:
                await bulk_copy(session, SalesOrderModel, orders)
                total_inserted += len(orders)
                print(f"   ⏳ Progress: {total_inserted:,}/{count:,}")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding orders: {e}")
                raise RuntimeError(
                    f"Sales order seeding aborted ({total_inserted:,}/{count:,} inserted): {e}"
                ) from e

        print(f"✅ {total_inserted:,} sales orders seeded successfully!")


if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(
        args, schema=SCHEMA_NAME, table=TABLE_NAME
    )

    asyncio.run(
        seed_sales_orders(
            count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos
        )
    )
