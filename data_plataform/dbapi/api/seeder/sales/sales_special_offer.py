import asyncio
import random
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.specialoffer import SpecialOfferModel

# Amostras
from api.samples.sales_special_offer import SPECIAL_OFFERS
from api.seeder.cli_common import build_parser, skip_if_already_seeded

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_special_offers():
    """Runs the special offers load (Master Data)."""
    async with AsyncSessionLocal() as session:
        if await skip_if_already_seeded(session, SpecialOfferModel, label="Special offers"):
            return

        print(f"📣 Seeding {len(SPECIAL_OFFERS)} special offers...")

        offers = []
        now = datetime.now()

        for item in SPECIAL_OFFERS:
            offers.append({
                **item,
                "start_date": now - timedelta(days=random.randint(1, 10)),
                "end_date": now + timedelta(days=random.randint(30, 90)),
                "is_active": True
            })

        try:
            # Bulk Insert
            stmt = insert(SpecialOfferModel).values(offers)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(offers)} special offers seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding offers: {e}")
            raise RuntimeError(f"Special offer seeding aborted: {e}") from e

if __name__ == "__main__":
    parser = build_parser("Seeder for sales.specialoffer (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_special_offers())
