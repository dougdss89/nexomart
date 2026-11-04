import asyncio
import os
import random
import sys
import uuid
from datetime import date, datetime
from typing import Any

from api.config.seeder_config import get_ranges
from api.database import AsyncSessionLocal
from api.models.person.customer import CustomerModel
from api.models.person.state_province import StateProvinceModel
from api.samples.customer import generate_mock_customer
from api.schemas.person.person_customer import Customer
from api.seeder.chaos import apply_customer_chaos, apply_false_foreigner_chaos
from api.utils.cli_common import add_volume_args, build_parser, bulk_copy, resolve_volume_args
from api.utils.quarantine import get_quarantine
from sqlalchemy import func, select

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "person"
TABLE_NAME = "person.customer"


def generate_customer_payloads(
    count: int, country_ids: list[int], states_by_country: dict[int, list[int]]
) -> list[dict[str, Any]]:
    """Generates customer payloads linked to REAL geographies (state_province_id always belongs to the drawn country_id)."""
    batch = []
    q = get_quarantine("person.customer")
    for _ in range(count):
        sample_data = generate_mock_customer()
        country_id = random.choice(country_ids)
        payload = {
            "customer_id": 1,  # placeholder for Pydantic validation (gt=0); removed before insert
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
            "row_guid": uuid.uuid4(),
        }
        with q.row(payload):
            customer_obj = Customer(**payload)
            validated_customer = customer_obj.model_dump()
            # demographics.metrics carries Decimal fields — needs a JSON-safe dump for the JSONB column
            validated_customer["demographics"] = customer_obj.demographics.model_dump(mode="json")
            validated_customer.pop(
                "customer_id", None
            )  # let the database's autoincrement take over
            batch.append(validated_customer)

    q.finish()
    return batch


async def seed_person_customers(
    count: int = 100, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True
):
    """Runs the customer load consuming REAL geographies from the database."""
    async with AsyncSessionLocal() as session:
        # 1. Fetch the map of states by country (Dimensional Integrity: state always belongs to the drawn country)
        states_result = (
            await session.execute(
                select(StateProvinceModel.state_province_id, StateProvinceModel.country_id)
            )
        ).all()
        states_by_country: dict[int, list[int]] = {}
        for state_id, country_id in states_result:
            states_by_country.setdefault(country_id, []).append(state_id)

        # Only draw countries that already have at least one registered state
        country_ids = list(states_by_country.keys())

        if not country_ids:
            print("⚠️ Country or StateProvince tables are empty! Run the geography seeders first.")
            return

        ranges = get_ranges(schema=SCHEMA_NAME, table=TABLE_NAME)
        existing_count = None
        if ranges.absolute_max:
            # COUNT(*) on this table scales with total accumulated history, not this run's
            # work — measured 11+ minutes against ~2M rows on a LIST-partitioned table, all
            # spent here before a single row of this run's actual work started. Only pay for
            # it when absolute_max is actually set (currently None for person.customer,
            # making the check a no-op) — same fix already applied to
            # purchase_purchaseorderdetail.py for the same reason.
            existing_count = (
                await session.scalar(select(func.count(CustomerModel.customer_id))) or 0
            )
            if existing_count >= ranges.absolute_max:
                print(
                    f"⚠️ Absolute customer limit reached ({existing_count:,}/{ranges.absolute_max:,})."
                )
                return
            count = min(count, ranges.absolute_max - existing_count)

        final_total = f"{existing_count + count:,}" if existing_count is not None else "n/a"
        print(
            f"📣 Seeding {count:,} customers (final total: {final_total}) | chaos={chaos_rate:.2%}"
        )

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            customers = generate_customer_payloads(
                current_batch_size, country_ids, states_by_country
            )

            if apply_chaos:
                customers = apply_customer_chaos(customers, chaos_rate=chaos_rate)
                customers = apply_false_foreigner_chaos(
                    customers, states_by_country, chaos_rate=chaos_rate
                )

            try:
                await bulk_copy(session, CustomerModel, customers)
                total_inserted += len(customers)
                print(f"   ⏳ Progress: {total_inserted:,}/{count:,}")
            except Exception as e:
                await session.rollback()
                print(f"❌ Error seeding customers: {e}")
                raise RuntimeError(
                    f"Customer seeding aborted ({total_inserted:,}/{count:,} inserted): {e}"
                ) from e

        print(f"✅ {total_inserted:,} customers seeded successfully!")


if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(
        args, schema=SCHEMA_NAME, table=TABLE_NAME
    )

    asyncio.run(
        seed_person_customers(
            count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos
        )
    )
