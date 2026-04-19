"""
EXEMPLO DE SEEDER COM CHAOS ENGINEERING APLICADO — DINÂMICO E RANDÔMICO

Este arquivo demonstra como usar as novas regras de geração de dados
com chaos engineering e volumes dinâmicos sorteados a cada execução.

NOVAS REGRAS APLICADAS:
- Volume DINÂMICO: sorteado entre 100k e 50kk a cada execução da DAG
- Chaos rate DINÂMICO: sorteado entre 1% e 10% a cada execução
- Tipos de chaos recomendados automaticamente por schema
- Validação de limites antes de inserir
- Totalmente parametrizável via CLI para uso no Airflow

USO NO AIRFLOW:
1. Com randomização total (recomendado):
   uv run python api/seeder/sales/sales_order_EXAMPLE_WITH_CHAOS.py --random

2. Com ranges customizados:
   uv run python api/seeder/sales/sales_order_EXAMPLE_WITH_CHAOS.py \
       --min-count 100000 --max-count 50000000 \
       --min-chaos 0.01 --max-chaos 0.10 \
       --random

3. Com valores fixos (desenvolvimento):
   uv run python api/seeder/sales/sales_order_EXAMPLE_WITH_CHAOS.py \
       --count 1000 --chaos-rate 0.05

4. Sem chaos (dados limpos):
   uv run python api/seeder/sales/sales_order_EXAMPLE_WITH_CHAOS.py \
       --count 1000 --no-chaos
"""

import uuid
import random
import asyncio
import sys
import os
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any
from sqlalchemy import select, insert, func
from api.database import AsyncSessionLocal

# Modelos
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.saleschannel import SalesChannelModel
from api.models.sales.specialoffer import SpecialOfferModel
from api.models.person.customer import CustomerModel
from api.models.person.seller import SellerModel
from api.models.product.productmodel import ProductModel

# Chaos Engineering
from api.utils.chaos_engineering import apply_data_quality_chaos, generate_chaos_metadata

# Configuração de Ranges Dinâmicos
from api.config.seeder_config import get_ranges, get_recommended_chaos_types

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))


# ==================== CONSTANTES ====================
DEFAULT_BATCH_SIZE = 10_000
SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"

# Obter ranges configurados para esta tabela
RANGES = get_ranges(table=TABLE_NAME)


# ==================== GERAÇÃO DE DADOS LIMPOS ====================
def generate_order_items(order_number: str, product_ids: List[int], count: int = 3) -> List[Dict[str, Any]]:
    """Gera itens de pedido (linhas) para uma ordem."""
    items = []
    for i in range(1, count + 1):
        p_id = random.choice(product_ids)
        qty = random.randint(1, 5)
        price = Decimal(random.uniform(10.0, 500.0)).quantize(Decimal("0.01"))
        items.append({
            "line_number": f"{order_number}-{i:02d}",
            "line_id": i,
            "product_id": p_id,
            "sku": f"SKU-{p_id:05d}",
            "quantity": qty,
            "unit_price": float(price),
            "seller_id": random.randint(1, 10),
            "delivery_info": {
                "status": "pending",
                "estimated_date": (datetime.now() + timedelta(days=5)).date().isoformat()
            },
            "taxes": [
                {"name": "ICMS", "rate": 0.18, "amount": float(price * qty * Decimal("0.18"))}
            ]
        })
    return items


def generate_sales_order_payloads(
    count: int,
    customer_ids: List[int],
    channel_ids: List[int],
    offer_ids: List[int],
    seller_ids: List[int],
    product_ids: List[int]
) -> List[Dict[str, Any]]:
    """
    Gera payloads de pedidos de venda LIMPOS (sem anomalias).
    As anomalias serão injetadas depois via apply_data_quality_chaos().
    """
    batch = []
    for _ in range(count):
        order_num = f"SO-{random.randint(100000000, 999999999)}"
        items = generate_order_items(order_num, product_ids, random.randint(1, 4))
        total_amount = sum(item["unit_price"] * item["quantity"] for item in items)

        order_date = datetime.now() - timedelta(days=random.randint(1, 30))

        batch.append({
            "salesorder_id": None,  # Auto-increment
            "salesordernumber": order_num,
            "customer_id": random.choice(customer_ids),
            "saleschannel_id": random.choice(channel_ids),
            "specialoffer_id": random.choice(offer_ids),
            "store_id": random.choice(seller_ids),
            "order_date": order_date,
            "order_status": "open",
            "total_amount": Decimal(total_amount).quantize(Decimal("0.01")),
            "currency_code": "BRL",
            "order_items": items,
            "payment_info": {
                "is_split": False,
                "transactions": [
                    {
                        "method": "pix",
                        "amount": float(total_amount),
                        "status": "paid",
                        "gateway_id": f"GW-{uuid.uuid4().hex[:6].upper()}"
                    }
                ]
            },
            "shipping_city": random.choice(["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba"]),
            "billing_city": random.choice(["São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba"]),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    return batch


# ==================== SEEDER PRINCIPAL ====================
async def seed_sales_orders(
    count: int,
    batch_size: int = DEFAULT_BATCH_SIZE,
    chaos_rate: float = 0.05,
    apply_chaos: bool = True
):
    """
    Executa a carga de pedidos de venda com chaos engineering DINÂMICO.

    Args:
        count: Quantidade de pedidos a gerar (pode ser sorteado dinamicamente)
        batch_size: Tamanho dos lotes para inserção
        chaos_rate: Percentual de anomalias (pode ser sorteado dinamicamente)
        apply_chaos: Se False, gera apenas dados limpos (útil para testes)
    """
    async with AsyncSessionLocal() as session:
        # 1. VALIDAÇÃO DE LIMITE MÁXIMO (se houver)
        existing_count = await session.scalar(select(func.count(SalesOrderModel.salesorder_id)))

        if RANGES.absolute_max:
            if existing_count >= RANGES.absolute_max:
                print(f"⚠️ Limite absoluto atingido ({existing_count:,}/{RANGES.absolute_max:,}).")
                return

            # Ajustar count para não ultrapassar limite
            count = min(count, RANGES.absolute_max - existing_count)
            print(f"📊 Volume ajustado: {count:,} registros (total final: {existing_count + count:,}/{RANGES.absolute_max:,})")

        # 2. BUSCAR FOREIGN KEYS (INTEGRIDADE DIMENSIONAL)
        customer_ids = (await session.execute(select(CustomerModel.customer_id))).scalars().all()
        channel_ids = (await session.execute(select(SalesChannelModel.saleschannel_id))).scalars().all()
        offer_ids = (await session.execute(select(SpecialOfferModel.special_offer_id))).scalars().all()
        seller_ids = (await session.execute(select(SellerModel.seller_id))).scalars().all()
        product_ids = (await session.execute(select(ProductModel.product_id))).scalars().all()

        if not all([customer_ids, channel_ids, offer_ids, seller_ids, product_ids]):
            print("⚠️ Tabelas base (Customers, Channels, Offers, Sellers, Products) vazias!")
            print("   Execute os seeders de dimensões primeiro.")
            return

        print(f"\n📣 Semeando {count:,} pedidos de venda (Fact Table)")
        print(f"   - Batch size: {batch_size:,}")
        print(f"   - Chaos rate: {chaos_rate:.2%}" if apply_chaos else "   - Chaos: DISABLED (dados limpos)")
        print(f"   - Registros existentes: {existing_count:,}\n")

        # 3. OBTER TIPOS DE CHAOS RECOMENDADOS
        chaos_types = get_recommended_chaos_types(SCHEMA_NAME)

        # 4. GERAR E INSERIR EM LOTES
        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)

            orders = generate_sales_order_payloads(
                current_batch_size,
                customer_ids,
                channel_ids,
                offer_ids,
                seller_ids,
                product_ids
            )

            # 5. APLICAR CHAOS ENGINEERING (se habilitado)
            if apply_chaos and chaos_types:
                orders_with_chaos = apply_data_quality_chaos(
                    orders,
                    chaos_rate=chaos_rate,
                    allowed_chaos_types=chaos_types,
                    verbose=False  # Verbose apenas no final
                )
            else:
                orders_with_chaos = orders

            # 6. INSERÇÃO EM LOTE (BULK INSERT - SEM FOR-LOOPS)
            try:
                stmt = insert(SalesOrderModel).values(orders_with_chaos)
                await session.execute(stmt)
                await session.commit()

                total_inserted += len(orders)

                # Progress indicator
                progress = (total_inserted / count) * 100
                print(f"   ⏳ Progresso: {total_inserted:,}/{count:,} ({progress:.1f}%)")

            except Exception as e:
                await session.rollback()
                print(f"   ❌ Erro ao inserir batch: {e}")
                break

        # 7. RESUMO FINAL
        print(f"\n✅ Seeder finalizado!")
        print(f"   - Registros inseridos: {total_inserted:,}")
        print(f"   - Total na tabela: {existing_count + total_inserted:,}")

        if apply_chaos and chaos_types:
            anomalies_count = int(total_inserted * chaos_rate)
            print(f"   - Anomalias injetadas: ~{anomalies_count:,} ({chaos_rate:.2%})")

        # 8. GERAR METADADOS DE CHAOS (para documentação)
        if apply_chaos and chaos_types:
            metadata = generate_chaos_metadata(
                table_name=TABLE_NAME,
                chaos_rate=chaos_rate,
                chaos_types=chaos_types
            )
            print(f"\n📊 Chaos Metadata: {metadata}")


# ==================== CLI ====================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"Seeder dinâmico para {TABLE_NAME} com Chaos Engineering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:

1. Modo randômico (recomendado para Airflow):
   python sales_order_EXAMPLE_WITH_CHAOS.py --random

2. Ranges customizados:
   python sales_order_EXAMPLE_WITH_CHAOS.py --min-count 100000 --max-count 1000000 --random

3. Valores fixos (desenvolvimento):
   python sales_order_EXAMPLE_WITH_CHAOS.py --count 1000 --chaos-rate 0.05

4. Sem chaos:
   python sales_order_EXAMPLE_WITH_CHAOS.py --count 1000 --no-chaos
        """
    )

    # Grupo 1: Modo randômico (recomendado)
    random_group = parser.add_argument_group('Modo Randômico (recomendado para DAGs)')
    random_group.add_argument(
        "--random",
        action="store_true",
        help="Sorteia valores dentro dos ranges configurados (min/max)"
    )
    random_group.add_argument(
        "--min-count",
        type=int,
        default=RANGES.min_count,
        help=f"Volume mínimo de registros (padrão: {RANGES.min_count:,})"
    )
    random_group.add_argument(
        "--max-count",
        type=int,
        default=RANGES.max_count,
        help=f"Volume máximo de registros (padrão: {RANGES.max_count:,})"
    )
    random_group.add_argument(
        "--min-chaos",
        type=float,
        default=RANGES.min_chaos,
        help=f"Chaos rate mínimo (padrão: {RANGES.min_chaos:.2%})"
    )
    random_group.add_argument(
        "--max-chaos",
        type=float,
        default=RANGES.max_chaos,
        help=f"Chaos rate máximo (padrão: {RANGES.max_chaos:.2%})"
    )

    # Grupo 2: Modo fixo (desenvolvimento)
    fixed_group = parser.add_argument_group('Modo Fixo (desenvolvimento/testes)')
    fixed_group.add_argument(
        "--count",
        type=int,
        help="Quantidade FIXA de registros (ignora randomização)"
    )
    fixed_group.add_argument(
        "--chaos-rate",
        type=float,
        help="Chaos rate FIXO (ignora randomização, 0.01 a 0.10)"
    )

    # Grupo 3: Configurações gerais
    general_group = parser.add_argument_group('Configurações Gerais')
    general_group.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Tamanho dos lotes para inserção (padrão: {DEFAULT_BATCH_SIZE:,})"
    )
    general_group.add_argument(
        "--no-chaos",
        action="store_true",
        help="Desabilita chaos engineering (gera apenas dados limpos)"
    )

    args = parser.parse_args()

    # ==================== LÓGICA DE RANDOMIZAÇÃO ====================

    if args.random:
        # MODO RANDÔMICO: sortear valores dentro dos ranges
        count = random.randint(args.min_count, args.max_count)
        chaos_rate = random.uniform(args.min_chaos, args.max_chaos) if not args.no_chaos else 0.0

        print("🎲 MODO RANDÔMICO ATIVADO")
        print(f"   📊 Range de volume: {args.min_count:,} a {args.max_count:,}")
        print(f"   📊 Range de chaos: {args.min_chaos:.2%} a {args.max_chaos:.2%}")
        print(f"\n   🎯 Valores sorteados:")
        print(f"      - Volume: {count:,} registros")
        print(f"      - Chaos rate: {chaos_rate:.2%}")

    else:
        # MODO FIXO: usar valores especificados ou defaults
        if args.count is None:
            # Se não especificou --count, sortear dentro do range (fallback)
            count = random.randint(RANGES.min_count, min(1000, RANGES.max_count))
            print(f"⚠️ --count não especificado. Usando valor randômico: {count:,}")
        else:
            count = args.count

        if args.chaos_rate is None:
            chaos_rate = RANGES.min_chaos if not args.no_chaos else 0.0
        else:
            chaos_rate = args.chaos_rate

    # Validar chaos_rate
    if chaos_rate > 0 and not (0.01 <= chaos_rate <= 0.10):
        print("⚠️ chaos_rate deve estar entre 0.01 (1%) e 0.10 (10%)")
        sys.exit(1)

    # Executar seeder
    asyncio.run(seed_sales_orders(
        count=count,
        batch_size=args.batch_size,
        chaos_rate=chaos_rate,
        apply_chaos=not args.no_chaos
    ))
