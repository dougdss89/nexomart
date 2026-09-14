import asyncio
import sys
import os

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.services.sales.sales_channel_service import SalesChannelService
from api.schemas.sales.sales_channel import SalesChannelSchema
from api.models.sales.saleschannel import SalesChannelModel
from api.samples.sales_channel import SALES_CHANNEL_SAMPLES
from api.seeder.cli_common import build_parser, skip_if_already_seeded

async def seed_sales_channels(count: int):
    """
    Seeder for Sales Channels.
    Imports data from the samples folder and uses the Service for bulk loading.
    """
    async with AsyncSessionLocal() as session:
        service = SalesChannelService(session)

        # Take from the sample, limiting by count if needed
        samples_to_use = SALES_CHANNEL_SAMPLES[:count]

        if await skip_if_already_seeded(session, SalesChannelModel, label="Sales channels"):
            return

        print(f"💰 Preparing load of {len(samples_to_use)} sales channels...")

        # Convert sample dicts into Pydantic schemas for validation
        payload = [SalesChannelSchema(**item) for item in samples_to_use]

        try:
            await service.seed_initial_channels(payload)
            await session.commit()
            print(f"✅ Seeder finished successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error during load: {e}")
            raise RuntimeError(f"Sales channel seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = build_parser("Seeder for sales.saleschannel (fixed master data, no chaos)")
    parser.add_argument("--count", type=int, default=5, help="Number of channels to load")
    args = parser.parse_args()

    asyncio.run(seed_sales_channels(args.count))
