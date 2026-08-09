import uuid
import asyncio
import sys
import os
from datetime import datetime, timedelta
import random
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.schemas.product.production_product_model import ProductModel
from api.models.product.productmodel import ProductModel as ProductModelTable
from api.seeder.cli_common import build_parser

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_model_seeders() -> list[dict]:

    batch = []
    days_elapsed = random.randint(30, 365)
    simulated_date = datetime.now() - timedelta(days=days_elapsed)

    for index, model in enumerate(ProductModel, start=1):

        batch.append(
            {
                "productmodel_id": index,
                "productmodel_name": model,
                "created_at": simulated_date,
                "updated_at": simulated_date,
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
        )

    return batch


async def seed_product_models():
    """Runs the product models load (Master Data — fixed list from the ProductModel Enum, no chaos)."""
    async with AsyncSessionLocal() as session:
        models = get_model_seeders()
        print(f"📣 Seeding {len(models)} product models...")
        try:
            stmt = insert(ProductModelTable).values(models)
            await session.execute(stmt)
            await session.commit()
            print("✅ Models seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding models: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for product.productmodel (fixed master data, no chaos)")
    parser.parse_args()

    asyncio.run(seed_product_models())
