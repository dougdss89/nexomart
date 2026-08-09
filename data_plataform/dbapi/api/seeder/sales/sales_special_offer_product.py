import random
import asyncio
import sys
import os
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.specialoffer import SpecialOfferModel
from api.models.product.product import Product
from api.models.sales.specialofferproduct import SpecialOfferProductModel
from api.seeder.cli_common import build_parser

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

async def seed_special_offer_products(avg_products_per_offer: int = 5):
    """Runs the linking of products to special offers with full integrity."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch valid IDs (Dimensional Integrity)
        offer_ids = (await session.execute(select(SpecialOfferModel.special_offer_id))).scalars().all()
        product_ids = (await session.execute(select(Product.product_id))).scalars().all()

        if not offer_ids or not product_ids:
            print("⚠️ SpecialOffer or Product tables are empty! Run the base seeders first.")
            return

        print(f"📣 Seeding links between {len(offer_ids)} offers and products...")

        mappings = []
        seen_pairs = set()

        for offer_id in offer_ids:
            # Select a random number of products for each offer
            num_products = random.randint(1, avg_products_per_offer * 2)
            selected_products = random.sample(product_ids, min(num_products, len(product_ids)))

            for product_id in selected_products:
                pair = (offer_id, product_id)
                if pair not in seen_pairs:
                    mappings.append({
                        "special_offer_id": offer_id,
                        "product_id": product_id
                    })
                    seen_pairs.add(pair)

        try:
            # Bulk Insert for performance
            stmt = insert(SpecialOfferProductModel).values(mappings)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(mappings)} offer/product links seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding links: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for sales.specialofferproduct (M:N fan-out of real offers x products, no chaos)")
    parser.add_argument("--avg-products-per-offer", type=int, default=5, help="Average products linked per offer")
    args = parser.parse_args()

    asyncio.run(seed_special_offer_products(avg_products_per_offer=args.avg_products_per_offer))
