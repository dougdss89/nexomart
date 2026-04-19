"""
Seeder Configuration — Dynamic Ranges per Schema

Este módulo define ranges dinâmicos de volume e chaos rate para cada schema.
Os valores são sorteados aleatoriamente a cada execução da DAG do Airflow.

Objetivo: Tornar os seeders imprevisíveis e realistas, simulando variações
de carga típicas de ambientes de produção (dias normais vs picos de demanda).
"""

from typing import Dict, Tuple
from dataclasses import dataclass


@dataclass
class SchemaRanges:
    """Define ranges min/max para volume e chaos rate de um schema."""
    min_count: int
    max_count: int
    min_chaos: float
    max_chaos: float
    absolute_max: int | None = None  # Limite absoluto (para product)
    description: str = ""


# ==================== RANGES POR SCHEMA ====================
SCHEMA_RANGES: Dict[str, SchemaRanges] = {
    "sales": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Simula desde dias normais (100k) até picos de demanda (Black Friday: 50kk)"
    ),

    "purchasing": SchemaRanges(
        min_count=50_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Volume combinado de headers + details com variação alta"
    ),

    "warehouse": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Movimentações de estoque com picos em períodos de alta demanda"
    ),

    "humanresources": SchemaRanges(
        min_count=50,
        max_count=100,
        min_chaos=0.0,  # Sem chaos em RH (dados críticos)
        max_chaos=0.0,
        description="Dimensão pequena e estável, sem chaos engineering"
    ),

    "product": SchemaRanges(
        min_count=500,
        max_count=1_500,
        min_chaos=0.02,
        max_chaos=0.05,
        absolute_max=100_000,  # LIMITE ABSOLUTO: 100k produtos
        description="Catálogo de produtos com limite absoluto de 100k"
    ),

    "person": SchemaRanges(
        min_count=10_000,
        max_count=5_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Clientes baseados em volume de vendas (1:N)"
    ),

    "marketing": SchemaRanges(
        min_count=1_000,
        max_count=500_000,
        min_chaos=0.01,
        max_chaos=0.08,
        description="Campanhas, ads e performance derivados de SKUs e períodos"
    ),
}


# ==================== RANGES POR TABELA ESPECÍFICA ====================
# Para casos onde uma tabela específica precisa de range diferente do schema

TABLE_RANGES: Dict[str, SchemaRanges] = {
    # Exemplos de override por tabela
    "sales.salesorder": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.01,
        max_chaos=0.10,
        description="Pedidos de venda (tabela fato principal)"
    ),

    "sales.sales_return_order": SchemaRanges(
        min_count=5_000,
        max_count=500_000,
        min_chaos=0.02,
        max_chaos=0.10,
        description="Devoluções (2-5% do volume de vendas)"
    ),

    "warehouse.transaction_history": SchemaRanges(
        min_count=200_000,
        max_count=50_000_000,
        min_chaos=0.02,
        max_chaos=0.10,
        description="Histórico de transações (múltiplas movimentações por pedido)"
    ),

    "warehouse.product_inventory": SchemaRanges(
        min_count=10_000,
        max_count=100_000,
        min_chaos=0.03,
        max_chaos=0.08,
        description="Inventário atual (1 registro por produto por localização)"
    ),

    "person.customer": SchemaRanges(
        min_count=50_000,
        max_count=5_000_000,
        min_chaos=0.03,
        max_chaos=0.10,
        description="Clientes (dimensão principal)"
    ),

    "person.address": SchemaRanges(
        min_count=50_000,
        max_count=10_000_000,
        min_chaos=0.05,
        max_chaos=0.10,
        description="Endereços (1:N com clientes - múltiplos endereços por cliente)"
    ),

    "product.product": SchemaRanges(
        min_count=500,
        max_count=1_500,
        min_chaos=0.02,
        max_chaos=0.05,
        absolute_max=100_000,
        description="Produtos (catálogo)"
    ),

    "product.product_category": SchemaRanges(
        min_count=20,
        max_count=50,
        min_chaos=0.0,
        max_chaos=0.0,
        description="Categorias de produtos (dimensão estável)"
    ),

    "humanresources.employee": SchemaRanges(
        min_count=50,
        max_count=100,
        min_chaos=0.0,
        max_chaos=0.0,
        description="Funcionários (dimensão pequena e crítica)"
    ),

    "humanresources.department": SchemaRanges(
        min_count=10,
        max_count=20,
        min_chaos=0.0,
        max_chaos=0.0,
        description="Departamentos (lookup table)"
    ),

    "purchasing.purchaseorderheader": SchemaRanges(
        min_count=50_000,
        max_count=10_000_000,
        min_chaos=0.01,
        max_chaos=0.08,
        description="Cabeçalhos de pedidos de compra"
    ),

    "purchasing.purchaseorderdetail": SchemaRanges(
        min_count=100_000,
        max_count=50_000_000,
        min_chaos=0.02,
        max_chaos=0.10,
        description="Detalhes de pedidos de compra (múltiplas linhas por header)"
    ),

    "marketing.marketing_campaign": SchemaRanges(
        min_count=100,
        max_count=5_000,
        min_chaos=0.02,
        max_chaos=0.05,
        description="Campanhas de marketing"
    ),

    "marketing.marketing_daily_performance": SchemaRanges(
        min_count=10_000,
        max_count=500_000,
        min_chaos=0.01,
        max_chaos=0.05,
        description="Performance diária de ads (time-series)"
    ),
}


# ==================== FUNÇÕES UTILITÁRIAS ====================

def get_ranges(schema: str = None, table: str = None) -> SchemaRanges:
    """
    Retorna ranges para um schema ou tabela específica.

    Args:
        schema: Nome do schema (ex: "sales")
        table: Nome completo da tabela (ex: "sales.salesorder")

    Returns:
        SchemaRanges com min/max para volume e chaos

    Example:
        >>> ranges = get_ranges(table="sales.salesorder")
        >>> print(ranges.min_count, ranges.max_count)
        100000 50000000
    """
    # Prioridade: tabela específica > schema > fallback padrão
    if table and table in TABLE_RANGES:
        return TABLE_RANGES[table]

    if schema and schema in SCHEMA_RANGES:
        return SCHEMA_RANGES[schema]

    # Fallback: range moderado para schemas não mapeados
    return SchemaRanges(
        min_count=1_000,
        max_count=100_000,
        min_chaos=0.02,
        max_chaos=0.08,
        description="Range padrão para schemas não configurados"
    )


def get_default_ranges(schema: str) -> Tuple[int, int, float, float]:
    """
    Retorna tupla (min_count, max_count, min_chaos, max_chaos) para um schema.

    Útil para uso direto em argparse defaults.

    Example:
        >>> min_c, max_c, min_ch, max_ch = get_default_ranges("sales")
        >>> parser.add_argument("--min-count", type=int, default=min_c)
    """
    ranges = get_ranges(schema=schema)
    return (ranges.min_count, ranges.max_count, ranges.min_chaos, ranges.max_chaos)


def validate_absolute_max(schema: str, table: str = None) -> int | None:
    """
    Retorna o limite absoluto para um schema/tabela (se houver).

    Args:
        schema: Nome do schema
        table: Nome completo da tabela (opcional)

    Returns:
        Limite absoluto (int) ou None se não houver limite

    Example:
        >>> max_products = validate_absolute_max("product", "product.product")
        >>> print(max_products)
        100000
    """
    ranges = get_ranges(schema=schema, table=table)
    return ranges.absolute_max


# ==================== CONFIGURAÇÕES DE CHAOS POR DOMÍNIO ====================

# Tipos de chaos recomendados por schema
RECOMMENDED_CHAOS_TYPES: Dict[str, list[str]] = {
    "sales": [
        "duplicate",
        "dirty_cpf",
        "dirty_email",
        "fraud_fake_cpf",
        "fraud_cloned_card",
        "fraud_high_value_suspicious",
        "incomplete_payment",
        "incomplete_shipping",
        "incomplete_delivery",
        "negative_values"
    ],

    "purchasing": [
        "duplicate",
        "fraud_duplicate_order",
        "fraud_phantom_vendor",
        "fraud_overpriced",
        "negative_cost",
        "missing_vendor_ref",
        "incomplete_delivery"
    ],

    "warehouse": [
        "warehouse_lost_transfer",
        "warehouse_quantity_mismatch",
        "warehouse_negative_stock",
        "warehouse_missing_location",
        "warehouse_quality_fail",
        "warehouse_cold_chain_break",
        "warehouse_same_location_transfer",
        "warehouse_missing_traceability",
        "missing_order_ref"
    ],

    "person": [
        "duplicate",
        "dirty_cpf",
        "dirty_phone",
        "dirty_email",
        "dirty_cep",
        "dirty_name",
        "fraud_fake_address"
    ],

    "product": [
        "duplicate",
        "outlier_value"
    ],

    "marketing": [
        "outlier_value",
        "outlier_quantity",
        "negative_values"
    ],

    "humanresources": []  # Sem chaos em RH
}


def get_recommended_chaos_types(schema: str) -> list[str]:
    """
    Retorna lista de tipos de chaos recomendados para um schema.

    Example:
        >>> chaos_types = get_recommended_chaos_types("sales")
        >>> print(chaos_types[:3])
        ['duplicate', 'dirty_cpf', 'dirty_email']
    """
    return RECOMMENDED_CHAOS_TYPES.get(schema, [])
