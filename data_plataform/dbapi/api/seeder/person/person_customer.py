import asyncio
import sys
import os
import uuid
import random
from datetime import datetime, date
from typing import List, Dict, Any
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.samples.customer import generate_mock_customer
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel
from api.models.person.customer import CustomerModel
from api.seeder.chaos import apply_customer_chaos, apply_false_foreigner_chaos
from api.seeder.cli_common import build_parser, add_volume_args, resolve_volume_args

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "person"
TABLE_NAME = "person.customer"

def generate_customer_payloads(count: int, country_ids: List[int], states_by_country: Dict[int, List[int]]) -> List[Dict[str, Any]]:
    """Generates customer payloads linked to REAL geographies (state_province_id always belongs to the drawn country_id)."""
    batch = []
    for _ in range(count):
        sample_data = generate_mock_customer()
        country_id = random.choice(country_ids)
        batch.append({
            "customer_name": sample_data["customer_name"],
            "last_name": sample_data["last_name"],
            "social_number": sample_data["social_number"],
            "birthdate": date.fromisoformat(sample_data["birthdate"]),
            "customer_type": sample_data["customer_type"],
            "credit_limit": sample_data["credit_limit"],
            "demographics": sample_data["demographics"],
            "country_id": country_id,
            "state_province_id": random.choice(states_by_country[country_id]),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

    return batch

async def seed_person_customers(count: int = 100, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the customer load consuming REAL geographies from the database."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch the map of states by country (Dimensional Integrity: state always belongs to the drawn country)
        states_result = (await session.execute(select(StateProvinceModel.state_province_id, StateProvinceModel.country_id))).all()
        states_by_country: Dict[int, List[int]] = {}
        for state_id, country_id in states_result:
            states_by_country.setdefault(country_id, []).append(state_id)

        # Only draw countries that already have at least one registered state
        country_ids = list(states_by_country.keys())

        if not country_ids:
            print("⚠️ Country or StateProvince tables are empty! Run the geography seeders first.")
            return

        print(f"📣 Seeding {count:,} customers | chaos={chaos_rate:.2%}")

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            customers = generate_customer_payloads(current_batch_size, country_ids, states_by_country)

            if apply_chaos:
                customers = apply_customer_chaos(customers, chaos_rate=chaos_rate)
                customers = apply_false_foreigner_chaos(customers, states_by_country, chaos_rate=chaos_rate)

            try:
                stmt = insert(CustomerModel).values(customers)
                await session.execute(stmt)
                await session.commit()
                total_inserted += len(customers)
                print(f"   ⏳ Progress: {total_inserted:,}/{count:,}")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding customers: {e}")
                break

        print(f"✅ {total_inserted:,} customers seeded successfully!")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_person_customers(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))