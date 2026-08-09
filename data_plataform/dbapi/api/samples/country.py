"""
Static data sample for the initial load of the CountryRegion table.
Isolates the raw data from the generation logic (Seeder).
"""

# Added the third tuple element for currency conversion (Local Currency)
OPERATING_COUNTRIES = [
    ("BR", "South America", "BRL"),
    ("AR", "South America", "ARS"),
    ("CO", "South America", "COP"),
    ("US", "North America", "USD"),
    ("CA", "North America", "CAD"),
    ("MX", "North America", "MXN"),
    ("PT", "Europe", "EUR"),
    ("ES", "Europe", "EUR"),
    ("FR", "Europe", "EUR"),
    ("DE", "Europe", "EUR"),
    ("JP", "Asia", "JPY"),
    ("AU", "Oceania", "AUD")
]