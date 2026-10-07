import asyncio
import sys
import os
import random
from datetime import date, datetime, timedelta
from sqlalchemy import select

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.warehouse.product_inventory_service import ProductInventoryService
from api.schemas.warehouse.warehouse_productinventory import ProductInventorySchema, InventoryAttributes
from api.models.product.product import Product
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.seeder.chaos import apply_inventory_chaos
from api.utils.cli_common import build_parser, add_volume_args, resolve_volume_args, stamp_dimension_history_objs

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.product_inventory"

async def seed_product_inventory(count: int, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """
    Seeder for Inventory Balances with Dimensional Integrity.
    Generates initial stock for REAL products and locations from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch valid IDs
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()
        location_ids = (await session.execute(select(WarehouseLocationModel.warehouselocation_id))).scalars().all()

        if not product_ids or not location_ids:
            print("⚠️ Product or WarehouseLocation tables are empty! Load canceled.")
            return

        service = ProductInventoryService(session)

        def generate_inventory_batch(n: int):
            batch_data = []
            for i in range(n):
                lot = f"LOT-{random.randint(100, 999)}-{random.choice(['A', 'B', 'C'])}"
                mfg_date = date.today() - timedelta(days=random.randint(30, 200))
                exp_date = mfg_date + timedelta(days=random.randint(365, 730))

                inv = ProductInventorySchema(
                    inventory_id=None, # Auto-increment
                    product_id=random.choice(product_ids),
                    warehouselocation_id=random.choice(location_ids),
                    quantity=random.randint(50, 1000),
                    bin_code=f"BIN-{random.choice(['A','B','C'])}-{random.randint(1, 999)}",
                    last_restock_date=datetime.now(),
                    attributes=InventoryAttributes(
                        lot_number=lot,
                        manufacturing_date=mfg_date,
                        expiration_date=exp_date
                    )
                )
                batch_data.append(inv)
            return batch_data

        print(f"📦 Injecting {count:,} inventory balance records | chaos={chaos_rate:.2%}")

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            batch_data = generate_inventory_batch(current_batch_size)

            if apply_chaos:
                batch_data = apply_inventory_chaos(batch_data, chaos_rate=chaos_rate)

            stamp_dimension_history_objs(batch_data)  # backdate created_at so the landing spans 2020->now
            try:
                await service.restock_inventory(batch_data)
                total_inserted += len(batch_data)
            except Exception as e:
                await session.rollback()
                print(f"❌ Error during load: {e}")
                raise RuntimeError(f"Product inventory seeding aborted ({total_inserted:,}/{count:,} inserted): {e}") from e

        try:
            await session.commit()
            print(f"✅ Seeder finished! {total_inserted:,} records inserted.")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error committing: {e}")
            raise

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_product_inventory(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
