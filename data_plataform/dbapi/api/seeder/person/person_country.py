import asyncio
import sys
import os
import uuid 
import pycountry
from datetime import datetime, timedelta
from typing import List, Dict, Any
from sqlalchemy import insert
from api.database import AsyncSessionLocal
from api.samples.country import OPERATING_COUNTRIES
from api.models.person.country_region import CountryRegionModel
from api.seeder.chaos import apply_geography_code_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def get_country_seeders(chaos_rate: float = 0.05, apply_chaos: bool = True) -> List[Dict[str, Any]]:
    """Generates the regional countries load."""
    batch = []
    setup_date = datetime.now() - timedelta(days=730)
    for index, (code, region, _currency) in enumerate(OPERATING_COUNTRIES, start=1):
        country = pycountry.countries.get(alpha_2=code)
        batch.append({
            "country_code": code,
            "countryregion": region,
            "country_name": country.name if country else code,
            "created_at": setup_date,
            "updated_at": setup_date,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    if apply_chaos:
        batch = apply_geography_code_chaos(batch, code_field="country_code", chaos_rate=chaos_rate)

    return batch

async def seed_person_countries(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the countries load into the database."""
    async with AsyncSessionLocal() as session:
        countries = get_country_seeders(chaos_rate=chaos_rate, apply_chaos=apply_chaos)
        print(f"📣 Seeding {len(countries)} countries | chaos={chaos_rate:.2%}")
        try:
            stmt = insert(CountryRegionModel).values(countries)
            await session.execute(stmt)
            await session.commit()
            print("✅ Countries seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding countries: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for person.country_region (fixed master data)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_person_countries(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
