import asyncio
import argparse
import sys
import os
import random
from datetime import datetime, timedelta

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.sales.sales_event_log_service import SalesEventLogService
from api.schemas.sales.sales_eventlog import SalesOrderEventLogSchema, SalesEventContext, GPSLocation
from api.samples.sales_eventlog import SALES_EVENT_LOG_TEMPLATES
from api.models.sales.salesorder import SalesOrderModel
from api.seeder.chaos import apply_eventlog_chaos
from api.utils.cli_common import add_chaos_args, resolve_chaos_args, sample_fk_ids

# Widened 2026-09-05 (was 1-15 days back) so event_date spans multiple years, matching
# the wider order_date range in sales_order.py.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)


async def seed_sales_eventlogs(count_orders: int, chaos_rate: float = 0.05, apply_chaos: bool = True):
    """
    Seeder for Order Event Logs with Dimensional Integrity.
    Generates realistic event trails linked to REAL orders from the database.
    """
    async with AsyncSessionLocal() as session:
        # 1. Fetch real orders to link the events to. sales.salesorder is unbounded/partitioned
        # (past 1M rows) — a full-column load here scales with total order history, not this
        # run's work, same class of bug as sales_order.py's customer_ids (fixed 2026-08-17
        # with this same helper). sample_fk_ids already returns a bounded random sample, so
        # no further random.sample() is needed on top of it.
        selected_orders = await sample_fk_ids(session, SalesOrderModel.salesorder_id, limit=count_orders)

        if not selected_orders:
            print("⚠️ No order (SalesOrder) found in the database! Run the SalesOrder seeder first.")
            return

        service = SalesEventLogService(session)

        all_events = []

        now = datetime.now()

        for order_id in selected_orders:
            # Decide how many events this order had (1 to 5 stages)
            stages = random.randint(1, len(SALES_EVENT_LOG_TEMPLATES))

            # Base date going back 1 day up to EARLIEST_TRANSACTION_DATE
            base_date = now - timedelta(days=random.randint(1, (now - EARLIEST_TRANSACTION_DATE).days))

            for i in range(stages):
                template = SALES_EVENT_LOG_TEMPLATES[i]

                # Each event occurs a few hours after the previous one to simulate a logistics flow
                # (capped at `now` so as not to violate the event_date <= created_at validation when
                # base_date is close to the present and the offsets accumulate past 24h)
                event_date = min(base_date + timedelta(hours=i * random.randint(2, 24)), now - timedelta(seconds=1))

                event = SalesOrderEventLogSchema(
                    event_id=None, # Auto-increment
                    order_id=order_id,
                    event_type=template["event_type"],
                    event_date=event_date,
                    event_data=SalesEventContext(
                        user_id=random.randint(1, 50),
                        reason=template["reason"],
                        affected_line_ids=[random.randint(1, 100) for _ in range(random.randint(1,3))],
                        gps_location=GPSLocation(
                            lat=random.uniform(-23.6, -23.4), # Simulating the São Paulo metropolitan region
                            long=random.uniform(-46.8, -46.5)
                        )
                    )
                )
                all_events.append(event)

        if apply_chaos:
            all_events = apply_eventlog_chaos(all_events, chaos_rate=chaos_rate)

        print(f"📈 Generating trails of {len(all_events)} events for {len(selected_orders)} real orders | chaos={chaos_rate:.2%}")

        try:
            await service.process_bulk_events(all_events)
            await session.commit()
            print(f"✅ Seeder finished successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error during load: {e}")
            raise RuntimeError(f"Sales event log seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder for Sales Event Logs (fan-out of real orders)")
    parser.add_argument("--orders", type=int, default=10, help="Number of orders to generate events for")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_sales_eventlogs(args.orders, chaos_rate=chaos_rate, apply_chaos=apply_chaos))
