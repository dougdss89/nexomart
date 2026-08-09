import asyncio
import sys
import os
import uuid
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.samples.state_province import STATES_BY_COUNTRY
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel
from api.seeder.chaos import apply_geography_code_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

def generate_state_payloads(country_map: Dict[str, int], chaos_rate: float = 0.05, apply_chaos: bool = True) -> List[Dict[str, Any]]:
    """Generates the states load linked to the database's countries."""
    batch = []
    setup_date = datetime.now()
    for country_code, states_list in STATES_BY_COUNTRY.items():
        current_country_id = country_map.get(country_code)
        if not current_country_id:
            continue
        for state_code, state_name in states_list:
            batch.append({
                "state_province_name": state_name,
                "state_province_code": state_code,
                "country_id": current_country_id,
                "created_at": setup_date,
                "updated_at": setup_date,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
    if apply_chaos:
        batch = apply_geography_code_chaos(batch, code_field="state_province_code", chaos_rate=chaos_rate)

    return batch

async def seed_person_states(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the states load linked to REAL countries from the database."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch the map of country codes to IDs (Dimensional Integrity)
        result = await session.execute(select(CountryRegionModel.country_id, CountryRegionModel.country_code))
        country_map = {row[1]: row[0] for row in result.all()}

        if not country_map:
            print("⚠️ No country found! Run the PersonCountry seeder first.")
            return

        print(f"📣 Seeding states for {len(country_map)} active countries | chaos={chaos_rate:.2%}")
        states = generate_state_payloads(country_map, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            stmt = insert(StateProvinceModel).values(states)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(states)} states seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding states: {e}")

if __name__ == "__main__":
    parser = build_parser("Seeder for person.state_province (fan-out of real countries)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_person_states(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
