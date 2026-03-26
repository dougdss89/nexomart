"""
Amostra estática de ofertas especiais (Promocionais).
Simula campanhas de marketing, descontos por volume e limpa-estoque.
"""

SPECIAL_OFFERS = [
    {
        "description": "Black Friday Antecipada",
        "discount_pct": 20.00,
        "discount_type": "seasonal",
        "category": "customer",
        "min_quantity": 1,
        "max_quantity": 5
    },
    {
        "description": "Desconto Progressivo Atacado",
        "discount_pct": 15.00,
        "discount_type": "volume",
        "category": "reseller",
        "min_quantity": 10,
        "max_quantity": 100
    },
    {
        "description": "Limpa Estoque - Fora de Linha",
        "discount_pct": 40.00,
        "discount_type": "discontinued",
        "category": "customer",
        "min_quantity": 1,
        "max_quantity": 2
    },
    {
        "description": "Lançamento Nova Coleção",
        "discount_pct": 10.00,
        "discount_type": "new_product",
        "category": "customer",
        "min_quantity": 1,
        "max_quantity": 10
    },
    {
        "description": "Semana do Consumidor",
        "discount_pct": 25.00,
        "discount_type": "seasonal",
        "category": "customer",
        "min_quantity": 1,
        "max_quantity": 3
    },
    {
        "description": "Combo Revenda Prata",
        "discount_pct": 5.00,
        "discount_type": "volume",
        "category": "reseller",
        "min_quantity": 50,
        "max_quantity": 500
    }
]
