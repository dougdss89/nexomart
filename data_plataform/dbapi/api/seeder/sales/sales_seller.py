import asyncio
import random
import sys
import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal

# Modelos
from api.models.person.seller import SellerModel
from api.models.person.country_region import CountryRegionModel

# Amostras
from api.samples.sales_seller import (
    SELLER_NAMES, TRADING_NAMES, BANK_CODES, BANK_NAMES,
    SELLER_TYPES, ADDRESS_STREETS, CITIES, STATES
)
from api.seeder.chaos import apply_seller_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.seller"

async def seed_sellers(count: int = 30, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the sellers load with full dimensional integrity."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch valid IDs (Dimensional Integrity)
        country_ids = (await session.execute(select(CountryRegionModel.country_id))).scalars().all()

        if not country_ids:
            print("⚠️ CountryRegion table is empty! Run the countries seeder first.")
            return

        print(f"📣 Seeding {count:,} sellers (Person.Seller) | chaos={chaos_rate:.2%}")
        sellers = []

        for i in range(count):
            name = SELLER_NAMES[i % len(SELLER_NAMES)]
            trading = TRADING_NAMES[i % len(TRADING_NAMES)]

            # Generate a realistic social_number (simulated CNPJ or CPF)
            s_type = random.choice(SELLER_TYPES)
            if s_type == "business":
                # CNPJ without punctuation (14 digits) — social_number is varchar(15); with
                # punctuation ("99.999.999/0001-99") it exceeds 18 characters and overflows the column.
                social = f"{random.randint(10, 99)}{random.randint(100, 999)}{random.randint(100, 999)}0001{random.randint(10, 99)}"
            else:
                social = f"{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}-{random.randint(10, 99)}"
            
            bank_idx = random.randint(0, len(BANK_CODES)-1)
            city_idx = random.randint(0, len(CITIES)-1)
            
            opened_date = datetime.now() - timedelta(days=random.randint(365, 3650))
            
            sellers.append({
                "seller_name": name,
                "last_name": "Co. & Partners" if s_type == "business" else name.split()[-1],
                "social_number": social,
                "opened_date": opened_date,
                "commission_rate": Decimal(random.uniform(0.05, 0.20)).quantize(Decimal("0.0001")),
                "country_id": random.choice(country_ids),
                "demographics": {
                    "information": {
                        "sellertype": s_type,
                        "trading_name": trading,
                        "support_email": f"support@{trading.lower()}.com.br"
                    },
                    "bank_account": {
                        "bank_code": BANK_CODES[bank_idx],
                        "bank_name": BANK_NAMES[bank_idx],
                        "agency": f"{random.randint(1000, 9999)}",
                        "account_number": f"{random.randint(10000, 99999)}-{random.randint(0, 9)}",
                        "account_type": "checking"
                    },
                    "address": [
                        {
                            "address_type": "office",
                            "address_line": f"{random.choice(ADDRESS_STREETS)}, {random.randint(1, 2000)}",
                            "postal_code": f"{random.randint(10000, 99999)}-{random.randint(0, 999)}",
                            "city": CITIES[city_idx],
                            "state_name": STATES[city_idx],
                            "country": "Brasil"
                        }
                    ],
                    "metrics": {
                        "average_rating": round(random.uniform(3.5, 5.0), 1),
                        "total_gross_revenue": round(random.uniform(10000.0, 500000.0), 2),
                        "total_items_sold": random.randint(100, 10000),
                        "count_orders": random.randint(50, 5000),
                        "chargeback_rate": round(random.uniform(0.0, 0.05), 4),
                        "date_first_sale": (opened_date + timedelta(days=30)).date().isoformat(),
                        "date_last_sale": date.today().isoformat()
                    }
                }
            })

        if apply_chaos:
            sellers = apply_seller_chaos(sellers, chaos_rate=chaos_rate)

        try:
            stmt = insert(SellerModel).values(sellers)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(sellers)} sellers seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding sellers: {e}")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_sellers(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
