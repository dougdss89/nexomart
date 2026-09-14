import random
import uuid
import asyncio
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import select, insert, func
from api.database import AsyncSessionLocal
from api.samples.product import generate_randomized_catalog
from api.schemas.product.production_product_category import ProductCategory
from api.schemas.product.production_product_model import ProductModel
from api.models.product.product import Product
from api.models.product.productcategory import ProductCategoryModel
from api.models.product.productmodel import ProductModel as ProductModelTable
from api.seeder.chaos import apply_product_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args
from api.config.seeder_config import get_ranges

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "product"
TABLE_NAME = "product.product"

# Widened 2026-09-05 (was 30-730 days back) so sell_start_date spans multiple years.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

def get_product_seeders(
    count: int = 100,
    start_id: int = 1,
    chaos_rate: float = 0.05,
    apply_chaos: bool = True,
    category_map: dict[str, int] | None = None,
    model_map: dict[str, int] | None = None,
) -> list[dict]:
    """
    Generates a massive batch of realistic products based on the simulated e-commerce store.

    category_map/model_map should come from a REAL query against the database (name -> id) —
    see seed_products(). Without them, the function still works (useful for isolated local
    tests such as product_embedding.py's __main__), but falls back to the fixed
    category_id=model_id=1, which is only safe if you know those rows exist.
    """
    batch = []
    category_map = category_map or {}
    model_map = model_map or {}

    # 1. Get the randomized, diversified virtual catalog ("Combinatorial Seeder")
    catalog_instances = generate_randomized_catalog(count)

    for offset, item in enumerate(catalog_instances):
        product_id = start_id + offset

        # Resolve the real Category and Model ID by querying what's in the database
        # (never by Enum position — insertion order isn't guaranteed)
        category_id = category_map.get(item["category"].value, 1)
        model_id = model_map.get(item["model"], 1)

        # Sale cycle: most are still on sale (sell_end_date=None)
        sell_start_date = datetime.now() - timedelta(days=random.randint(30, (datetime.now() - EARLIEST_TRANSACTION_DATE).days))

        # 2. Assemble the dict (maps perfectly to SQLAlchemy and JSONB)
        batch.append({
            "product_id": product_id,
            "product_name": item["product_name"][:100],
            "model_id": model_id,
            "category_id": category_id,

            # SQLAlchemy automatically converts this inner dict to JSONB
            "productdetails": item["details"],

            "sell_start_date": sell_start_date,
            "sell_end_date": None,

            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        })

    if apply_chaos:
        batch = apply_product_chaos(batch, chaos_rate=chaos_rate)

    return batch


async def seed_products(count: int = 500, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the products load respecting the absolute catalog limit (100k)."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch REAL Categories and Models from the database (Dimensional Integrity —
        # never infer category_id/model_id by Enum position)
        category_map = dict((await session.execute(
            select(ProductCategoryModel.category_name, ProductCategoryModel.category_id)
        )).all())
        model_map = dict((await session.execute(
            select(ProductModelTable.productmodel_name, ProductModelTable.productmodel_id)
        )).all())

        if not category_map or not model_map:
            print("⚠️ ProductCategory or ProductModel tables are empty! Run those seeders first.")
            return

        existing_count = await session.scalar(select(func.count(Product.product_id))) or 0
        max_id = await session.scalar(select(func.max(Product.product_id))) or 0

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        if ranges.absolute_max and existing_count >= ranges.absolute_max:
            print(f"⚠️ Absolute product limit reached ({existing_count:,}/{ranges.absolute_max:,}).")
            return
        if ranges.absolute_max:
            count = min(count, ranges.absolute_max - existing_count)

        print(f"📣 Seeding {count:,} products (final total: {existing_count + count:,}) | chaos={chaos_rate:.2%}")

        next_id = max_id + 1
        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            products = get_product_seeders(
                current_batch_size, start_id=next_id, chaos_rate=chaos_rate, apply_chaos=apply_chaos,
                category_map=category_map, model_map=model_map,
            )
            next_id += current_batch_size

            try:
                stmt = insert(Product).values(products)
                await session.execute(stmt)
                await session.commit()
                total_inserted += len(products)
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding products: {e}")
                raise RuntimeError(f"Product seeding aborted ({total_inserted:,}/{count:,} inserted): {e}") from e

        print(f"✅ {total_inserted:,} products seeded successfully!")


if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_products(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
