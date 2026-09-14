import asyncio
import sys
import os
import random
from datetime import datetime, timedelta
from decimal import Decimal

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from sqlalchemy import select, func
from api.database import AsyncSessionLocal
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema, TraceabilityInfo, IoTSensorData
from api.models.product.product import Product
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.models.warehouse.transactiontype import WarehouseTransactionType
from api.models.warehouse.transactionhistory import TransactionHistoryModel
from api.models.sales.salesorder import SalesOrderModel
from api.seeder.chaos import apply_transaction_history_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args, generate_unique_business_ids, bulk_copy, sample_fk_ids
from api.config.seeder_config import get_ranges

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.transaction_history"

# Widened 2026-09-05 (was 0-90 days back) so transaction_date spans multiple years.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

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
        # sales.salesorder is unbounded/partitioned and already past 2M rows — a full-column
        # load here scales with total order history, not this run's work (same class of bug
        # as sales_order.py's customer_ids, fixed 2026-08-17 with the same helper). Every ID
        # returned is still a real, existing order — only where the sampling happens changed.
        order_ids = await sample_fk_ids(session, SalesOrderModel.salesorder_id)

        if not product_ids or not location_ids or not txn_type_ids or not order_ids:
            # reference_order_id is NOT NULL on transactionhistory — there's no orderless
            # fallback, so an empty SalesOrder table must hard-stop the seeder instead of
            # generating rows that always violate the constraint (see incident 2026-08-13:
            # a truncate left salesorder empty and this looped forever on batch failures).
            print("⚠️ Base tables are empty! Make sure to run the Product, Location, TransactionType, and SalesOrder seeders first.")
            return

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) scales with total accumulated history, not this run's work — only pay
            # for it when absolute_max is actually set (currently None for this table, making
            # the check a no-op). Same fix as purchase_purchaseorderdetail.py/person_customer.py.
            existing_count = await session.scalar(select(func.count(TransactionHistoryModel.transaction_id))) or 0
            if existing_count >= ranges.absolute_max:
                print(f"⚠️ Absolute transaction history limit reached ({existing_count:,}/{ranges.absolute_max:,}).")
                return
            count = min(count, ranges.absolute_max - existing_count)

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

        def build_batch(transaction_ids: list[str]) -> list[TransactionHistorySchema]:
            batch = []
            for transaction_id in transaction_ids:
                qty = random.randint(1, 50) # Reduced for more realism (inventory balance)
                cost = Decimal(random.uniform(10.0, 1200.0)).quantize(Decimal("0.0001"))

                type_id = random.choice(txn_type_ids)
                source_location_id = random.choice(location_ids)
                # Source and target can't be the same location (schema validator) —
                # only draws a target if another location is available.
                other_locations = [l for l in location_ids if l != source_location_id]
                target_location_id = random.choice(other_locations) if (type_id != 2 and other_locations) else None

                txn = TransactionHistorySchema(
                    transaction_id=transaction_id,
                    product_id=random.choice(product_ids),
                    reference_order_id=random.choice(order_ids),
                    reference_order_line_id=random.randint(1, 10),
                    transaction_date=datetime.now() - timedelta(days=random.randint(0, (datetime.now() - EARLIEST_TRANSACTION_DATE).days)),
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
        MAX_CONSECUTIVE_BATCH_FAILURES = 5
        consecutive_batch_failures = 0

        while total_created < count:
            current_batch_size = min(batch_size, count - total_created)

            # transaction_id collisions used to be the common failure mode here (see
            # generate_unique_business_ids' docstring for the birthday-paradox math) —
            # IDs are now pre-checked against the DB before the batch is built, so a
            # collision at INSERT time should be vanishingly rare. The retry loop stays
            # as a safety net for that rare case (or any other transient batch error):
            # each batch commits on its own, so a failure only costs that batch, not the
            # whole load.
            for attempt in range(1, MAX_ATTEMPTS + 1):
                transaction_ids = await generate_unique_business_ids(
                    session, TransactionHistoryModel.transaction_id,
                    prefix="TR", count=current_batch_size,
                )
                batch_data = build_batch(transaction_ids)

                if apply_chaos:
                    batch_data = apply_transaction_history_chaos(batch_data, cold_storage_location_ids, neutral_type_ids, chaos_rate=chaos_rate)

                print(f"📦 Injecting batch of {current_batch_size} transactions (Dimensional Integrity OK)...")

                try:
                    # bulk_copy (Solution B) instead of the repository/service layer for this
                    # hot path — model_dump() gives the same plain-dict shape the repository's
                    # bulk_insert() used to receive; traceability_info (JSONB) is handled by
                    # bulk_copy's own json.dumps() pass.
                    await bulk_copy(session, TransactionHistoryModel, [s.model_dump() for s in batch_data])
                    total_created += current_batch_size
                    consecutive_batch_failures = 0
                    break
                except Exception as e:
                    await session.rollback()
                    print(f"❌ Error in batch (attempt {attempt}/{MAX_ATTEMPTS}, likely transaction_id collision): {e}")
            else:
                consecutive_batch_failures += 1
                print(f"⚠️ Batch discarded after {MAX_ATTEMPTS} attempts — moving on to the next batch.")
                if consecutive_batch_failures >= MAX_CONSECUTIVE_BATCH_FAILURES:
                    print(f"❌ {consecutive_batch_failures} consecutive batches failed — aborting instead of looping forever.")
                    raise RuntimeError(
                        f"transaction_history seeder aborted after {consecutive_batch_failures} consecutive batch failures "
                        f"({total_created:,}/{count:,} inserted)"
                    )

        print(f"✅ Seeder finished! {total_created} audited records inserted.")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME, default_batch_size=500)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_transaction_history(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
