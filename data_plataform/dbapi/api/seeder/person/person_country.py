import asyncio
import os
import sys
import uuid
from datetime import datetime, timedelta
from typing import Any

import pycountry
from api.database import AsyncSessionLocal
from api.models.person.country_region import CountryRegionModel
from api.samples.country import OPERATING_COUNTRIES
from api.schemas.person.person_country_region import CountryRegion
from api.seeder.chaos import apply_geography_code_chaos
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


def get_country_seeders(chaos_rate: float = 0.05, apply_chaos: bool = True) -> list[dict[str, Any]]:
    """Generates the regional countries load."""
    batch = []
    q = get_quarantine("person.countryregion")
    setup_date = datetime.now() - timedelta(days=730)
    for index, (code, region, _currency) in enumerate(OPERATING_COUNTRIES, start=1):
        country = pycountry.countries.get(alpha_2=code)
        payload = {
            "country_id": index,  # placeholder for Pydantic validation (gt=0); removed before insert
            "country_code": code,
            "countryregion": region,
            "country_name": country.name if country else code,
            "created_at": setup_date,
            "updated_at": setup_date,
            "is_active": True,
            "row_guid": uuid.uuid4(),
        }
        with q.row(payload):
            validated_country = CountryRegion(**payload).model_dump()
            validated_country.pop("country_id", None)  # let the database's autoincrement take over
            batch.append(validated_country)
    if apply_chaos:
        batch = apply_geography_code_chaos(batch, code_field="country_code", chaos_rate=chaos_rate)

    q.finish()
    return batch


async def seed_person_countries(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the countries load into the database."""
    async with AsyncSessionLocal() as session:
        countries = get_country_seeders(chaos_rate=chaos_rate, apply_chaos=apply_chaos)
        if await skip_if_already_seeded(session, CountryRegionModel, label="Countries"):
            return
        print(f"📣 Seeding {len(countries)} countries | chaos={chaos_rate:.2%}")
        countries = physical_gate(CountryRegionModel, countries)
        try:
            stmt = insert(CountryRegionModel).values(stamp_dimension_history(countries))
            await session.execute(stmt)
            await session.commit()
            print("✅ Countries seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding countries: {e}")
            raise RuntimeError(f"Country seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for person.country_region (fixed master data)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_person_countries(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
