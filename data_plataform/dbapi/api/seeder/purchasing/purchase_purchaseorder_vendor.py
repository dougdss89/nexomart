import uuid
import random
import asyncio
import sys
import os
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import select, func, insert
from api.database import AsyncSessionLocal

# Import the Pydantic schema for on-the-fly validation
from api.schemas.purchasing.purchasing_vendor import Vendor
from api.models.purchasing.vendor import PurchaseVendorModel
import api.samples.purchase_payment_certs as purchase_payment_certs
from api.seeder.chaos import apply_vendor_chaos
from api.utils.cli_common import build_parser, add_volume_args, resolve_volume_args, stamp_dimension_history

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

SCHEMA_NAME = "purchasing"
TABLE_NAME = "purchasing.vendor"

# Widened 2026-09-05 (was 30-1000 days back) so the SCD history below spans multiple years.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)

fake = Faker('pt_BR')

def get_vendor_details(num_vendors: int, start_id: int = 1, chaos_rate: float = 0.03, apply_chaos: bool = True) -> list[dict]:
    """ Generates the vendor catalog with History (SCD2) and Chaos Engineering (Data Quality) """

    batch = []
    vendor_counter_id = start_id  # business_entity_id/account_number: stable per vendor
    row_id = start_id  # vendor_id (real PK) needs to be unique per ROW, even in SCD2

    for _ in range(num_vendors):
        company_name = fake.company()
        # Clean standard formatting
        cnpj = fake.cnpj()

        # --- CHAOS ENGINEERING (Data Quality) ---
        # 10% chance of a dirty CNPJ (no formatting)
        if random.random() < 0.10:
            cnpj = cnpj.replace(".", "").replace("-", "").replace("/", "")

        # Vendor contact
        phone = fake.phone_number()
        contact_email = fake.email()

        # 5% chance of a missing email or phone (simulating frontend issues)
        if random.random() < 0.05:
            contact_email = "not_provided@domain.com"
        if random.random() < 0.05:
            phone = "0000000000"

        payment_term = random.choice(purchase_payment_certs.PAYMENT_TERMS)
        certification = random.choice(purchase_payment_certs.POSSIBLE_CERTIFICATIONS)

        has_history = random.choice([True, False, False, False, True])
        today = datetime.now() - timedelta(days=random.randint(30, (datetime.now() - EARLIEST_TRANSACTION_DATE).days))

        if has_history:
            # --- SCD TYPE 2 LOGIC (Credit Risk Decline) ---
            old_start = today - timedelta(days=random.randint(600, 1000))
            old_end = today - timedelta(days=random.randint(100, 500))

            # In the past, the vendor was good (High Rating) and Active
            old_rating = random.randint(4, 5)

            old_payload = {
                "vendor_id": row_id,
                "business_entity_id": vendor_counter_id, # In this model, we simplify to 1:1
                "account_number": f"ACC-{vendor_counter_id:06d}",
                "vendor_name": company_name,
                "credit_rating": old_rating,
                "vendor_details": {
                    "tax_id": cnpj,
                    "payment_terms": payment_term,
                    "certifications": random.choices(purchase_payment_certs.POSSIBLE_CERTIFICATIONS, k=random.randint(1, 3)),
                    "primary_contact": {
                        "name": fake.name(),
                        "email": contact_email,
                        "phone": phone
                    },
                    "average_lead_time_days": random.randint(5, 45),
                    "on_time_delivery_rate": round(random.uniform(0.7, 0.99), 2)
                },
                "purchasing_webservice_url": f"https://api.{fake.domain_name()}/orders",

                "effective_start_date": old_start,
                "effective_end_date": old_end,
                "is_current": False,

                "created_at": old_start,
                "updated_at": old_end,
                "is_active": True,
                "row_guid": uuid.uuid4()
            }

            # Strict validation via Pydantic
            old_vendor = Vendor(**old_payload).model_dump()
            old_vendor["purchasing_webservice_url"] = str(old_vendor["purchasing_webservice_url"])
            batch.append(old_vendor)
            row_id += 1

            # CURRENT: the rating dropped to 1 or 2.
            # Since is_active defaults to True, the Pydantic validator would block it!
            # We need to force the deactivation in the current generation.
            current_start = old_end + timedelta(days=random.randint(1, 30))
            current_rating = random.randint(1, 2)
            current_active = False # Forced deactivation due to high risk (approved by the Pydantic validator)

        else:
            # --- NEW VENDOR (No History) ---
            current_start = today - timedelta(days=random.randint(1, 30))
            current_rating = random.randint(3, 5) # New vendors start out good
            current_active = True

        current_payload = {
            "vendor_id": row_id,
            "business_entity_id": vendor_counter_id,
            "account_number": f"ACC-{vendor_counter_id:06d}",
            "vendor_name": company_name,
            "credit_rating": current_rating,
            "vendor_details": {
                "tax_id": cnpj,
                "payment_terms": payment_term,
                # May have gained new certifications
                "certifications": random.choices(purchase_payment_certs.POSSIBLE_CERTIFICATIONS, k=random.randint(1, 3)),
                "primary_contact": {
                    "name": fake.name(),
                    "email": contact_email,
                    "phone": phone
                },
                "average_lead_time_days": random.randint(5, 45),
                "on_time_delivery_rate": round(random.uniform(0.7, 0.99), 2)
            },
            "purchasing_webservice_url": f"https://api.{fake.domain_name()}/orders",

            "effective_start_date": current_start,
            "effective_end_date": None,
            "is_current": True,

            "created_at": current_start,
            "updated_at": current_start,
            "is_active": current_active,
            "row_guid": uuid.uuid4()
        }

        # Goes through Pydantic's fine mesh (business rule validation)
        current_vendor = Vendor(**current_payload).model_dump()
        current_vendor["purchasing_webservice_url"] = str(current_vendor["purchasing_webservice_url"])
        batch.append(current_vendor)
        row_id += 1

        vendor_counter_id += 1

    if apply_chaos:
        batch = apply_vendor_chaos(batch, chaos_rate=chaos_rate)

    return batch


async def seed_vendors(count: int = 20, chaos_rate: float = 0.03, batch_size: int = 10_000, apply_chaos: bool = True):
    """Runs the vendors load (Master Data with SCD2 history)."""
    async with AsyncSessionLocal() as session:
        max_id = await session.scalar(select(func.max(PurchaseVendorModel.vendor_id))) or 0
        print(f"📣 Seeding {count:,} vendors (SCD2) | chaos={chaos_rate:.2%}")

        vendors = get_vendor_details(count, start_id=max_id + 1, chaos_rate=chaos_rate, apply_chaos=apply_chaos)
        stamp_dimension_history(vendors)  # backdate created_at so the landing spans 2020->now

        try:
            for i in range(0, len(vendors), batch_size):
                stmt = insert(PurchaseVendorModel).values(vendors[i:i + batch_size])
                await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(vendors)} vendor records seeded successfully (includes SCD2 history)!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding vendors: {e}")
            raise RuntimeError(f"Vendor seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(seed_vendors(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
