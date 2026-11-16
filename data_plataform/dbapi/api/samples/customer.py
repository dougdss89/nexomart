import math
import random
from datetime import date, timedelta
from typing import Any

from api.samples.person_demographics import INTERESTS_BANK, OCCUPATIONS, ROLES
from faker import Faker

fake = Faker("pt_BR")


def _derive_metrics(is_company: bool, first: date | None = None) -> dict:
    """
    Derives the purchase metrics from one latent engagement level per customer,
    so orders, ticket, income and recency are correlated instead of independent.

    `first` pins the first-purchase date (used when re-deriving metrics for an existing
    customer, so the date already landed in the lake does not change).
    """
    today = date.today()
    if first is None:
        first = fake.date_between(start_date=date(2020, 1, 1), end_date="today")

    # Latent variable: one engagement level per customer drives everything
    engagement = random.lognormvariate(0, 0.5)
    pj_boost = 1.6 if is_company else 1.0

    # Engaged customers bought recently: the gap since the last purchase shrinks with engagement
    gap_days = min(int(random.expovariate(engagement / 400)), (today - first).days)
    last = today - timedelta(days=gap_days)
    tenure_months = max((last - first).days / 30.0, 1.0)

    # Resample instead of clamping, so there are no point masses at 50k / 200k
    while True:
        max_income = random.lognormvariate(math.log(80_000), 0.35) * pj_boost * engagement**0.3
        if 50_000.0 <= max_income <= 200_000.0:
            break
    min_income = max_income * random.uniform(0.3, 0.6)

    orders_per_month = 0.5 * engagement * (2.0 if is_company else 1.0)
    count_orders = max(
        1, min(150, round(orders_per_month * tenure_months * random.lognormvariate(0, 0.25)))
    )

    ticket = max_income * 0.0015 * random.lognormvariate(0, 0.4) * pj_boost
    total_purchased = round(count_orders * ticket, 2)

    return {
        "minimum_year_income": round(min_income, 2),
        "maximum_year_income": round(max_income, 2),
        "total_purchased": total_purchased,
        "date_of_first_purchase": first.isoformat(),
        "date_last_purchase": last.isoformat(),
        "count_orders": count_orders,
    }


def generate_mock_customer() -> dict[str, Any]:
    """
    Generates a payload simulating a real Customer,
    with properties nested inside demographics.
    """
    # Basic
    is_company = random.choice([True, False])
    customer_type = "VC" if is_company else "SC"

    if is_company:
        customer_name = fake.company()
        last_name = fake.company_suffix()
        # Formatted CNPJ ("99.999.999/9999-99") has 18 characters — doesn't fit in
        # social_number varchar(15). Always unpunctuated here (14 digits).
        social_number = fake.cnpj().replace(".", "").replace("/", "").replace("-", "")
        gender = None
    else:
        gender = random.choice(["M", "F", "Other"])
        if gender == "M":
            customer_name = fake.first_name_male()
            last_name = fake.last_name_male()
        elif gender == "F":
            customer_name = fake.first_name_female()
            last_name = fake.last_name_female()
        else:
            customer_name = fake.first_name()
            last_name = fake.last_name()

        # Anomaly 1: CPF variation
        if random.random() < 0.5:
            social_number = fake.cpf()
        else:
            social_number = fake.cpf().replace(".", "").replace("-", "")

    birthdate = fake.date_of_birth(minimum_age=18, maximum_age=90)
    credit_limit = round(random.uniform(500.0, 50000.0), 2)

    # --- DEMOGRAPHICS (JSONB) ---
    social = {
        "gender": gender,
        "marital_status": random.choice(["Single", "Married", "Divorced", "Widowed"])
        if not is_company
        else None,
        "interests": random.sample(INTERESTS_BANK, k=random.randint(1, 4)),
        "occupation": random.choice(OCCUPATIONS) if not is_company else None,
        "company_name": customer_name if is_company else fake.company(),
        "tax_regime": random.choice(["simple", "real_profit", "presumed_profit"])
        if is_company
        else None,
    }

    if is_company:
        social["buying_contact"] = {
            "name": fake.name(),
            "role": random.choice(ROLES),
            "phone_number": fake.phone_number(),
        }

    marketing_consent = {"email": random.choice([True, False]), "sms": random.choice([True, False])}

    address = {
        "address_line": fake.street_name(),
        "postal_code": fake.postcode(),
        "street_number": fake.building_number(),
        "city": fake.city(),
        "state_name": fake.state(),
        "country": "Brazil",
        "country_region": "South America",
        "province_name": fake.bairro(),
        "address_info": random.choice(["Apto 101", "Casa 2", "Fundos", ""]),
    }

    metrics = _derive_metrics(is_company)

    demographics = {
        "social": social,
        "marketing_consent": marketing_consent,
        "address": address,
        "metrics": metrics,
    }

    # --- INTENTIONAL DATA ENGINEERING ANOMALIES ---

    # 1. Negative Metrics Bug: 2% chance of the purchased value coming in negative
    if random.random() < 0.02:
        demographics["metrics"]["total_purchased"] = -abs(
            demographics["metrics"]["total_purchased"]
        )

    # 2. Schema Drift: 5% chance of injecting a mysterious metric (LTV) not covered by the official Pydantic schema
    if random.random() < 0.05:
        demographics["metrics"]["lifetime_value_score"] = random.randint(1, 100)

    # 3. Missing Sub-Document: 3% chance of losing marketing consent due to a frontend API error
    if random.random() < 0.03:
        demographics.pop("marketing_consent", None)

    # 4. Text Inconsistency (Typos): Names in random Caps Lock
    if random.random() < 0.04:
        customer_name = customer_name.upper()
        last_name = last_name.lower()

    return {
        "customer_name": customer_name,
        "last_name": last_name,
        "social_number": social_number,
        "birthdate": birthdate.isoformat(),
        "customer_type": customer_type,
        "credit_limit": credit_limit,
        "demographics": demographics,
    }
