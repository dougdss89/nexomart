import uuid
import asyncio
import sys
import os
from datetime import datetime, timedelta
import random
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.schemas.product.production_product_category import ProductCategory
from api.models.product.productcategory import ProductCategoryModel
from api.seeder.chaos import apply_category_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_category_seeders(chaos_rate: float = 0.0, apply_chaos: bool = False) -> list[dict]:

    batch = []
    days_elapsed = random.randint(30, 365)
    simulated_date = datetime.now() - timedelta(days=days_elapsed)

    for index, category in enumerate(ProductCategory, start=1):

        batch.append(
            {
                "category_id": index,
                "category_name": category.value,
                "parent_category_id": None,
                "category_identifier": uuid.uuid4(),
                "created_date": simulated_date,
            }
        )

    if apply_chaos:
        batch = apply_category_chaos(batch, chaos_rate=chaos_rate)

    return batch


async def seed_product_categories(chaos_rate: float = 0.0, apply_chaos: bool = False):
    """Runs the categories load (Master Data — fixed list from the ProductCategory Enum)."""
    async with AsyncSessionLocal() as session:
        categories = get_category_seeders(chaos_rate=chaos_rate, apply_chaos=apply_chaos)
        print(f"📣 Seeding {len(categories)} categories | chaos={chaos_rate:.2%}")
        try:
            stmt = insert(ProductCategoryModel).values(categories)
            await session.execute(stmt)
            await session.commit()
            print("✅ Categories seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding categories: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for product.productcategory (fixed master data)")
    add_chaos_args(parser, default_chaos_rate=0.0)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.0)

    asyncio.run(seed_product_categories(chaos_rate=chaos_rate, apply_chaos=apply_chaos))