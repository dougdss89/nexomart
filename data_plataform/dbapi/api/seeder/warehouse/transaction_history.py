import asyncio
import sys
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from sqlalchemy import select
from api.database import AsyncSessionLocal
from api.services.warehouse.transaction_history_service import TransactionHistoryService
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema, TraceabilityInfo, IoTSensorData
from api.models.product.product import Product
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.models.warehouse.transactiontype import WarehouseTransactionType
from api.models.sales.salesorder import SalesOrderModel
from api.seeder.chaos import apply_transaction_history_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.transaction_history"

async def seed_transaction_history(count: int, chaos_rate: float = 0.05, batch_size: int = 500, apply_chaos: bool = True):
    """
    Complex seeder for Transaction History.
    Generates simulated movements tying REAL products and orders from the database together.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch valid IDs to ensure referential integrity
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()
        location_ids = (await session.execute(select(WarehouseLocationModel.warehouselocation_id))).scalars().all()
        txn_type_ids = (await session.execute(select(WarehouseTransactionType.transactiontype_id))).scalars().all()
        order_ids = (await session.execute(select(SalesOrderModel.salesorder_id))).scalars().all()

        if not product_ids or not location_ids or not txn_type_ids:
            print("⚠️ Base tables are empty! Make sure to run the Product, Location, and TransactionType seeders first.")
            return

        # Cold Storage locations (for the cold chain break chaos)
        location_types = (await session.execute(
            select(WarehouseLocationModel.warehouselocation_id, WarehouseLocationModel.capabilities["warehouse_type"].astext)
        )).all()
        cold_storage_location_ids = {loc_id for loc_id, wtype in location_types if wtype == "cold_storage"}

        # Transaction types with a 'neutral' effect (transfers, for the logistics black hole chaos)
        type_effects = (await session.execute(
            select(WarehouseTransactionType.transactiontype_id, WarehouseTransactionType.action_effect)
        )).all()
        neutral_type_ids = {type_id for type_id, effect in type_effects if effect == "neutral"}

        service = TransactionHistoryService(session)

        def build_batch(batch_size: int) -> list[TransactionHistorySchema]:
            batch = []
            for i in range(batch_size):
                qty = random.randint(1, 50) # Reduced for more realism (inventory balance)
                cost = Decimal(random.uniform(10.0, 1200.0)).quantize(Decimal("0.0001"))

                type_id = random.choice(txn_type_ids)
                source_location_id = random.choice(location_ids)
                # Source and target can't be the same location (schema validator) —
                # only draws a target if another location is available.
                other_locations = [l for l in location_ids if l != source_location_id]
                target_location_id = random.choice(other_locations) if (type_id != 2 and other_locations) else None

                txn = TransactionHistorySchema(
                    transaction_id=f"TR-{random.randint(100000000, 999999999)}",
                    product_id=random.choice(product_ids),
                    reference_order_id=random.choice(order_ids) if order_ids else None,
                    reference_order_line_id=random.randint(1, 10) if order_ids else None,
                    transaction_date=datetime.now() - timedelta(days=random.randint(0, 90)),
                    transactiontype_id=type_id,
                    quantity=qty,
                    actual_cost=cost,
                    traceability_info=TraceabilityInfo(
                        reason="Movimentação de estoque (Simulação Auditada)",
                        performed_by_user=f"operador_{random.randint(1, 20)}@dataplatform.com",
                        source_warehouselocation_id=source_location_id,
                        target_warehouselocation_id=target_location_id,
                        quality_check_status=random.choices(["pass", "fail", "hold"], weights=[90, 5, 5])[0],
                        iot_sensor_data=IoTSensorData(
                            temp_at_scan=random.uniform(15.0, 25.0),
                            device_id=f"IOT-SCANNER-{random.randint(100, 999)}"
                        )
                    )
                )
                batch.append(txn)
            return batch

        total_created = 0
        MAX_ATTEMPTS = 3

        while total_created < count:
            current_batch_size = min(batch_size, count - total_created)

            # transaction_id is a random 9-digit numeric suffix (~900M possible values,
            # convention documented in CLAUDE.md) — with tens of millions of rows already
            # in the table, a PK collision stops being rare. Previously, a single collision
            # in ANY batch triggered SQLAlchemy's automatic rollback (because of flush()
            # with no per-batch commit), which discarded EVERY earlier batch that had
            # already been flushed-but-not-committed in the same session, and the printed
            # counter never reflected what actually made it to the database. Now each
            # batch commits on its own and retries with fresh IDs on collision, so a
            # one-off failure only costs that batch, not the whole load.
            for attempt in range(1, MAX_ATTEMPTS + 1):
                batch_data = build_batch(current_batch_size)

                if apply_chaos:
                    batch_data = apply_transaction_history_chaos(batch_data, cold_storage_location_ids, neutral_type_ids, chaos_rate=chaos_rate)

                print(f"📦 Injecting batch of {current_batch_size} transactions (Dimensional Integrity OK)...")

                try:
                    await service.process_bulk_transactions(batch_data)
                    await session.commit()
                    total_created += current_batch_size
                    break
                except Exception as e:
                    await session.rollback()
                    print(f"❌ Error in batch (attempt {attempt}/{MAX_ATTEMPTS}, likely transaction_id collision): {e}")
            else:
                print(f"⚠️ Batch discarded after {MAX_ATTEMPTS} attempts — moving on to the next batch.")

        print(f"✅ Seeder finished! {total_created} audited records inserted.")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME, default_batch_size=500)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_transaction_history(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
