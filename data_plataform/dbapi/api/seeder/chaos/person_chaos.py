"""
Chaos engineering para os seeders de person (customer, customer_email, geografia).
Ver dbapi/api/seeder/CHAOS_DOCUMENTATION.md secao 1 (Anomalias de Qualidade de Dados)
para o catalogo completo.
"""

import random
from datetime import date
from typing import Any


def apply_customer_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Nome fora do padrao (upper/lower/espacos extras) e datas de nascimento ilogicas."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        chaos_type = random.choice([
            "all_caps", "all_lower", "extra_whitespace", "future_birthdate", "impossible_age",
        ])

        if chaos_type == "all_caps":
            row["customer_name"] = row["customer_name"].upper()
            row["last_name"] = row["last_name"].upper()

        elif chaos_type == "all_lower":
            row["customer_name"] = row["customer_name"].lower()
            row["last_name"] = row["last_name"].lower()

        elif chaos_type == "extra_whitespace":
            row["customer_name"] = f"  {row['customer_name']}  "
            row["last_name"] = f"  {row['last_name']}  "

        elif chaos_type == "future_birthdate":
            row["birthdate"] = date(
                random.randint(2030, 2050), random.randint(1, 12), random.randint(1, 28)
            ).isoformat()

        elif chaos_type == "impossible_age":
            row["birthdate"] = date(
                date.today().year - random.randint(120, 150), random.randint(1, 12), random.randint(1, 28)
            ).isoformat()

    return batch


def apply_customer_email_chaos(batch: list[dict[str, Any]], chaos_rate: float = 0.05) -> list[dict[str, Any]]:
    """Virgula no lugar do ponto, TLD ausente e multi-valor escondido no mesmo campo."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        email = row.get("customer_email", "")
        if "@" not in email:
            continue

        chaos_type = random.choice(["comma_instead_of_dot", "missing_tld", "hidden_multivalue"])
        user, domain = email.rsplit("@", 1)

        if chaos_type == "comma_instead_of_dot" and "." in domain:
            row["customer_email"] = f"{user}@{domain.replace('.', ',', 1)}"

        elif chaos_type == "missing_tld" and "." in domain:
            row["customer_email"] = f"{user}@{domain.rsplit('.', 1)[0]}"

        elif chaos_type == "hidden_multivalue":
            second_email = f"{user}.work@empresa.com"
            row["customer_email"] = f"{email}; {second_email}"

    return batch


def apply_geography_code_chaos(
    batch: list[dict[str, Any]], code_field: str, chaos_rate: float = 0.05
) -> list[dict[str, Any]]:
    """Codigos ISO/estado enviados em minusculo, comum em integracoes de terceiros."""
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    for idx in chaos_indices:
        row = batch[idx]
        if row.get(code_field):
            row[code_field] = row[code_field].lower()

    return batch


def apply_false_foreigner_chaos(
    batch: list[dict[str, Any]],
    states_by_country: dict[int, list[int]],
    chaos_rate: float = 0.02,
) -> list[dict[str, Any]]:
    """'Falso Estrangeiro': state_province_id aponta para um pais diferente do country_id."""
    if not batch or len(states_by_country) < 2:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))
    countries = list(states_by_country.keys())

    for idx in chaos_indices:
        row = batch[idx]
        wrong_countries = [c for c in countries if c != row.get("country_id")]
        if not wrong_countries:
            continue
        wrong_country = random.choice(wrong_countries)
        row["state_province_id"] = random.choice(states_by_country[wrong_country])

    return batch
