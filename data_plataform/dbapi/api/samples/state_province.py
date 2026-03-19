"""
Amostra estática de Estados e Províncias.
Mapeado pelo código Alpha-2 do país para facilitar o vínculo das chaves estrangeiras.
"""

ESTADOS_OPERACAO = {
    "BR": [
        ("SP", "São Paulo"),
        ("RJ", "Rio de Janeiro"),
        ("MG", "Minas Gerais"),
        ("PR", "Paraná"),
        ("SC", "Santa Catarina")
    ],
    "US": [
        ("CA", "California"),
        ("NY", "New York"),
        ("TX", "Texas"),
        ("FL", "Florida")
    ],
    "CA": [
        ("ON", "Ontario"),
        ("BC", "British Columbia"),
        ("QC", "Quebec")
    ],
    "PT": [
        ("11", "Lisboa"), # Em Portugal, distritos usam números no ISO 3166-2
        ("13", "Porto"),
        ("03", "Braga")
    ]
    # Você pode expandir para os outros países depois...
}