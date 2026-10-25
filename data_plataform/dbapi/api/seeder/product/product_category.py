import asyncio
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

from api.database import AsyncSessionLocal
from api.models.product.productcategory import ProductCategoryModel
from api.schemas.product.production_product_category import ProductCategory, ProductCategorySchema
from api.seeder.chaos import apply_category_chaos
from api.utils.cli_common import (
    add_chaos_args,
    build_parser,
    resolve_chaos_args,
    skip_if_already_seeded,
    stamp_dimension_history,
)
from api.utils.quarantine import get_quarantine, physical_gate
from sqlalchemy import insert

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)


def get_category_seeders(chaos_rate: float = 0.0, apply_chaos: bool = False) -> list[dict]:
    batch = []
    q = get_quarantine("product.productcategory")
    # Widened 2026-09-05 (was 30-365 days back) so created_date spans multiple years.
    days_elapsed = random.randint(30, (datetime.now() - EARLIEST_TRANSACTION_DATE).days)
    simulated_date = datetime.now() - timedelta(days=days_elapsed)

    for index, category in enumerate(ProductCategory, start=1):
        payload = {
            "category_id": index,
            "category_name": category.value,
            "parent_category_id": None,
            "category_identifier": uuid.uuid4(),
            "created_date": simulated_date,
        }
        with q.row(payload):
            batch.append(ProductCategorySchema(**payload).model_dump())

    if apply_chaos:
        batch = apply_category_chaos(batch, chaos_rate=chaos_rate)

    q.finish()
    return batch


async def seed_product_categories(chaos_rate: float = 0.0, apply_chaos: bool = False):
    """Runs the categories load (Master Data — fixed list from the ProductCategory Enum)."""
    async with AsyncSessionLocal() as session:
        categories = get_category_seeders(chaos_rate=chaos_rate, apply_chaos=apply_chaos)
        if await skip_if_already_seeded(session, ProductCategoryModel, label="Categories"):
            return
        print(f"📣 Seeding {len(categories)} categories | chaos={chaos_rate:.2%}")
        categories = physical_gate(ProductCategoryModel, categories)
        try:
            stmt = insert(ProductCategoryModel).values(stamp_dimension_history(categories))
            await session.execute(stmt)
            await session.commit()
            print("✅ Categories seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding categories: {e}")
            raise RuntimeError(f"Category seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for product.productcategory (fixed master data)")
    add_chaos_args(parser, default_chaos_rate=0.0)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.0)

    asyncio.run(seed_product_categories(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
