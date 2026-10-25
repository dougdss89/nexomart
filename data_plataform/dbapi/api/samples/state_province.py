"""
Static sample of states and provinces.
Mapped by the country's Alpha-2 code to make foreign key linking easier.
"""

STATES_BY_COUNTRY = {
    "BR": [
        ("SP", "São Paulo"),
        ("RJ", "Rio de Janeiro"),
        ("MG", "Minas Gerais"),
        ("PR", "Paraná"),
        ("SC", "Santa Catarina"),
    ],
    "US": [("CA", "California"), ("NY", "New York"), ("TX", "Texas"), ("FL", "Florida")],
    "CA": [("ON", "Ontario"), ("BC", "British Columbia"), ("QC", "Quebec")],
    "PT": [
        ("11", "Lisboa"),  # In Portugal, districts use numbers in ISO 3166-2
        ("13", "Porto"),
        ("03", "Braga"),
    ],
    "CL": [("RM", "Región Metropolitana"), ("VS", "Valparaíso"), ("BI", "Biobío")],
    "PE": [("LIM", "Lima"), ("ARE", "Arequipa"), ("CUS", "Cusco")],
    "IT": [("LOM", "Lombardia"), ("LAZ", "Lazio"), ("CAM", "Campania")],
    # You can expand to other countries later...
}
