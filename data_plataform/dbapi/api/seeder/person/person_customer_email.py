import uuid
import random
import asyncio
import sys
import os
from datetime import datetime
from typing import List, Dict, Any
from faker import Faker
from sqlalchemy import select, insert, func
from api.database import AsyncSessionLocal
from api.models.person.customer import CustomerModel
from api.models.person.customer_email import CustomerEmailModel
from api.schemas.person.person_customer_email import Customer_email
from api.seeder.chaos import apply_customer_email_chaos
from api.utils.cli_common import build_parser, add_chaos_args, resolve_chaos_args, bulk_copy
from api.utils.quarantine import get_quarantine
from api.config.seeder_config import get_ranges

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

    q = get_quarantine("person.customer_email")
    payload = {
        "customer_id": customer.get("customer_id", 0), # Will be injected by the orchestrator
        "customer_email": primary_email,
        "is_primary": True,
    }
    with q.row(payload):
        emails.append(Customer_email(**payload).model_dump())

    # 2. Generate secondary emails (20% chance)
    if random.random() < 0.20:
        sec_email = f"financeiro@{domain}" if is_company else f"spam_{base_name}@{domain}"

        payload = {
            "customer_id": customer.get("customer_id", 0),
            "customer_email": sec_email,
            "is_primary": False,
        }
        with q.row(payload):
            emails.append(Customer_email(**payload).model_dump())

    return emails

def get_customer_email_seeders(customers: List[Dict[str, Any]], chaos_rate: float = 0.05, apply_chaos: bool = True) -> List[Dict[str, Any]]:
    """
    Receives REAL customers from the database (with customer_id/customer_name/customer_type
    already populated) and generates the emails linked to them.
    """
    batch = []
    q = get_quarantine("person.customer_email")
    for customer in customers:
        batch.extend(generate_mock_emails(customer))

    if apply_chaos:
        batch = apply_customer_email_chaos(batch, chaos_rate=chaos_rate)

    q.finish()
    return batch


async def seed_customer_emails(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Generates emails for REAL customers in the database that don't have any email registered yet.

    Root-cause fix (2026-08-17, revised same day): first attempt used an anti-join
    (`LEFT JOIN ... WHERE customer_email.customer_id IS NULL`) instead of loading both full
    columns into Python — correct, but still measured at 3m+ against ~2M accumulated
    customers: Postgres has no index that identifies *which* customers lack an email, so a
    Nested Loop Anti Join still has to walk customer rows roughly in physical/insertion order
    until it accumulates enough misses, i.e. it still scans through most of the *matched*
    rows first. Since customer_id is a simple increasing integer and every run only ever
    processes customers created at-or-before that run, "pending" is always exactly "customer_id
    greater than the highest customer_id that already has an email" — a plain indexed range
    scan (see ix_customer_email_customer_id, used here for the MAX()), no join needed at all.
    Correct as long as a run either fully commits its email batch or rolls back entirely on
    error (true here — see the `except`/`raise` below), so the high-water mark never advances
    past a customer that didn't actually get an email.
    """
    fan_out_limit = get_ranges(schema="person", table="person.customer").max_count

    async with AsyncSessionLocal() as session:
        watermark = await session.scalar(select(func.max(CustomerEmailModel.customer_id))) or 0
        pending_stmt = (
            select(CustomerModel.customer_id, CustomerModel.customer_name, CustomerModel.customer_type)
            .where(CustomerModel.customer_id > watermark)
            .order_by(CustomerModel.customer_id)
            .limit(fan_out_limit)
        )
        result = await session.execute(pending_stmt)
        customers = [
            {"customer_id": r[0], "customer_name": r[1], "customer_type": r[2]}
            for r in result.all()
        ]

        if not customers:
            print("⚠️ No customer pending an email! Run the Customer seeder first (or everyone already has an email).")
            return

        print(f"📣 Generating emails for {len(customers)} real customers | chaos={chaos_rate:.2%}")
        emails = get_customer_email_seeders(customers, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await bulk_copy(session, CustomerEmailModel, emails)
            print(f"✅ {total} emails seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding emails: {e}")
            raise RuntimeError(f"Customer email seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for person.customer_email (fan-out of real customers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_customer_emails(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
