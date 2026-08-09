import uuid
import random
import asyncio
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from faker import Faker
from sqlalchemy import select, insert
from api.database import AsyncSessionLocal
from api.models.person.customer import CustomerModel
from api.models.person.customer_email import CustomerEmailModel
from api.seeder.chaos import apply_customer_email_chaos
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args, insert_in_batches

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

fake = Faker('pt_BR')

def generate_mock_emails(customer: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Generates the email rows for a single customer.
    Can generate 1 or more emails.
    Injects anomalies (typos, missing @, dirty domains).
    """
    emails = []

    # Determine the domain based on the customer type
    is_company = customer.get("customer_type") == "VC"
    base_name = str(customer.get("customer_name")).lower().replace(" ", "").replace(".", "").replace(",", "")

    # 1. Generate the primary email
    primary_email = ""
    if is_company:
        domain = f"{base_name}.com.br"
        primary_email = f"contato@{domain}"
    else:
        domain = random.choice(["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br"])
        primary_email = f"{base_name}.{random.randint(1950, 2010)}@{domain}"

    # --- PRIMARY EMAIL ANOMALIES ---
    if random.random() < 0.03:
        # Formatting error: missing '@'
        primary_email = primary_email.replace("@", "")
    elif random.random() < 0.02:
        # Formatting error: space in the email
        primary_email = primary_email.replace("@", " @")

    emails.append({
        "customer_id": customer.get("customer_id", 0), # Will be injected by the orchestrator
        "customer_email": primary_email,
        "is_primary": True,
        "email_uuid": str(uuid.uuid4()),
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "is_active": True,
        "row_guid": str(uuid.uuid4())
    })

    # 2. Generate secondary emails (20% chance)
    if random.random() < 0.20:
        sec_email = f"financeiro@{domain}" if is_company else f"spam_{base_name}@{domain}"
        
        emails.append({
            "customer_id": customer.get("customer_id", 0),
            "customer_email": sec_email,
            "is_primary": False,
            "email_uuid": str(uuid.uuid4()),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        })
        
    return emails

def get_customer_email_seeders(customers: List[Dict[str, Any]], chaos_rate: float = 0.05, apply_chaos: bool = True) -> List[Dict[str, Any]]:
    """
    Receives REAL customers from the database (with customer_id/customer_name/customer_type
    already populated) and generates the emails linked to them.
    """
    try:
        batch = []
        for customer in customers:
            batch.extend(generate_mock_emails(customer))

        if apply_chaos:
            batch = apply_customer_email_chaos(batch, chaos_rate=chaos_rate)

        return batch

    except Exception as e:
        print(f"Error generating email seeders: {e}")
        return []


async def seed_customer_emails(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Generates emails for REAL customers in the database that don't have any email registered yet."""
    async with AsyncSessionLocal() as session:
        already_have_email = set((await session.execute(select(CustomerEmailModel.customer_id))).scalars().all())

        result = await session.execute(select(CustomerModel.customer_id, CustomerModel.customer_name, CustomerModel.customer_type))
        customers = [
            {"customer_id": r[0], "customer_name": r[1], "customer_type": r[2]}
            for r in result.all() if r[0] not in already_have_email
        ]

        if not customers:
            print("⚠️ No customer pending an email! Run the Customer seeder first (or everyone already has an email).")
            return

        print(f"📣 Generating emails for {len(customers)} real customers | chaos={chaos_rate:.2%}")
        emails = get_customer_email_seeders(customers, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await insert_in_batches(session, CustomerEmailModel, emails)
            print(f"✅ {total} emails seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding emails: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for person.customer_email (fan-out of real customers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_customer_emails(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
