"""
DAG EXEMPLO — Seeders com Randomização Dinâmica

Esta DAG demonstra como executar seeders com volumes e chaos rates
DINÂMICOS e IMPREVISÍVEIS a cada execução, ideal para simular
cenários realistas de engenharia de dados.

CARACTERÍSTICAS:
- Volumes sorteados dentro de ranges configurados
- Chaos rate sorteado entre 1% e 10%
- Execução em paralelo de múltiplos schemas
- Dependências respeitadas (dimensões antes de fatos)
- Logs detalhados para monitoramento

SCHEDULE:
- Executa diariamente às 02:00 (horário de baixa demanda)
- Pode ser ajustado conforme necessidade

OBSERVAÇÃO:
Esta é uma DAG de EXEMPLO. Para produção, ajustar:
1. SEEDER_BASE_PATH para o caminho correto no seu ambiente
2. Schedule conforme política de carga do projeto
3. Retries e timeouts conforme SLA
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import random


# ==================== CONFIGURAÇÕES ====================

# Caminho base dos seeders (ajustar conforme seu ambiente)
SEEDER_BASE_PATH = "/home/doug/project/data_plataform/dbapi/api/seeder"

# Ranges de volume por schema (podem ser customizados por tabela)
SCHEMA_RANGES = {
    "humanresources": {"min": 50, "max": 100, "min_chaos": 0.0, "max_chaos": 0.0},
    "product": {"min": 500, "max": 1500, "min_chaos": 0.02, "max_chaos": 0.05},
    "person": {"min": 10_000, "max": 5_000_000, "min_chaos": 0.03, "max_chaos": 0.10},
    "sales": {"min": 100_000, "max": 50_000_000, "min_chaos": 0.01, "max_chaos": 0.10},
    "purchasing": {"min": 50_000, "max": 50_000_000, "min_chaos": 0.01, "max_chaos": 0.10},
    "warehouse": {"min": 100_000, "max": 50_000_000, "min_chaos": 0.02, "max_chaos": 0.10},
    "marketing": {"min": 1_000, "max": 500_000, "min_chaos": 0.01, "max_chaos": 0.08},
}

# Configuração da DAG
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email": ["data-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ==================== FUNÇÕES AUXILIARES ====================

def generate_seeder_command(seeder_path: str, schema: str) -> str:
    """
    Gera comando bash para executar um seeder com randomização.

    Args:
        seeder_path: Caminho relativo do seeder (ex: "sales/sales_order.py")
        schema: Nome do schema (para buscar ranges configurados)

    Returns:
        Comando bash completo com ranges dinâmicos
    """
    ranges = SCHEMA_RANGES.get(schema, {"min": 1000, "max": 100_000, "min_chaos": 0.02, "max_chaos": 0.08})

    return f"""
    cd {SEEDER_BASE_PATH} && \\
    uv run python {seeder_path} \\
        --random \\
        --min-count {ranges['min']} \\
        --max-count {ranges['max']} \\
        --min-chaos {ranges['min_chaos']} \\
        --max-chaos {ranges['max_chaos']}
    """


def log_execution_params(**context):
    """
    Loga os parâmetros da execução (para auditoria e debugging).
    """
    execution_date = context['execution_date']
    dag_run = context['dag_run']

    print(f"🎲 Execução da DAG: {dag_run.dag_id}")
    print(f"   📅 Data de execução: {execution_date}")
    print(f"   🔀 Run ID: {dag_run.run_id}")
    print(f"\n📊 Ranges configurados:")

    for schema, ranges in SCHEMA_RANGES.items():
        print(f"   {schema}:")
        print(f"      - Volume: {ranges['min']:,} a {ranges['max']:,}")
        print(f"      - Chaos: {ranges['min_chaos']:.2%} a {ranges['max_chaos']:.2%}")


# ==================== DAG DEFINITION ====================

with DAG(
    dag_id="example_seeder_dynamic_randomization",
    default_args=DEFAULT_ARGS,
    description="Executa seeders com volumes e chaos rates dinâmicos e randômicos",
    schedule_interval="0 2 * * *",  # Diariamente às 02:00
    catchup=False,
    tags=["seeder", "data-quality", "chaos-engineering", "example"],
) as dag:

    # Task inicial: Log de execução
    log_start = PythonOperator(
        task_id="log_execution_params",
        python_callable=log_execution_params,
        provide_context=True,
    )

    # ==================== DIMENSÕES (Tabelas Base) ====================
    # Executar em paralelo (sem dependências entre si)

    with TaskGroup(group_id="seed_dimensions") as seed_dimensions:

        # HumanResources (sem chaos)
        seed_hr_department = BashOperator(
            task_id="seed_hr_department",
            bash_command=generate_seeder_command(
                "humanresources/humanresources_department.py",
                "humanresources"
            ),
        )

        seed_hr_employee = BashOperator(
            task_id="seed_hr_employee",
            bash_command=generate_seeder_command(
                "humanresources/humanresources_employee.py",
                "humanresources"
            ),
        )

        # Product (chaos moderado)
        seed_product_category = BashOperator(
            task_id="seed_product_category",
            bash_command=generate_seeder_command(
                "product/product_category.py",
                "product"
            ),
        )

        seed_product = BashOperator(
            task_id="seed_product",
            bash_command=generate_seeder_command(
                "product/product.py",
                "product"
            ),
        )

        seed_product_category >> seed_product  # Categoria antes de produto

        # Person (dimensão principal)
        seed_person_country = BashOperator(
            task_id="seed_person_country",
            bash_command=generate_seeder_command(
                "person/person_country.py",
                "person"
            ),
        )

        seed_person_customer = BashOperator(
            task_id="seed_person_customer",
            bash_command=generate_seeder_command(
                "person/person_customer.py",
                "person"
            ),
        )

        seed_person_country >> seed_person_customer

        # Sales (lookup tables)
        seed_sales_channel = BashOperator(
            task_id="seed_sales_channel",
            bash_command=generate_seeder_command(
                "sales/sales_channel.py",
                "sales"
            ),
        )

        seed_sales_seller = BashOperator(
            task_id="seed_sales_seller",
            bash_command=generate_seeder_command(
                "sales/sales_seller.py",
                "sales"
            ),
        )

        # Warehouse (lookup tables)
        seed_warehouse_location = BashOperator(
            task_id="seed_warehouse_location",
            bash_command=generate_seeder_command(
                "warehouse/warehouse_location.py",
                "warehouse"
            ),
        )

        seed_transaction_type = BashOperator(
            task_id="seed_transaction_type",
            bash_command=generate_seeder_command(
                "warehouse/transaction_type.py",
                "warehouse"
            ),
        )

        # Purchasing (vendors)
        seed_purchasing_vendor = BashOperator(
            task_id="seed_purchasing_vendor",
            bash_command=generate_seeder_command(
                "purchasing/purchase_purchaseorder_vendor.py",
                "purchasing"
            ),
        )

    # ==================== FATOS (Tabelas Principais) ====================
    # Dependem das dimensões

    with TaskGroup(group_id="seed_facts") as seed_facts:

        # Sales Orders (fato principal)
        seed_sales_order = BashOperator(
            task_id="seed_sales_order",
            bash_command=generate_seeder_command(
                "sales/sales_order_EXAMPLE_WITH_CHAOS.py",  # Usar exemplo
                "sales"
            ),
        )

        # Sales Returns (baseado em orders)
        seed_sales_return = BashOperator(
            task_id="seed_sales_return",
            bash_command=generate_seeder_command(
                "sales/sales_return_order.py",
                "sales"
            ),
        )

        seed_sales_order >> seed_sales_return

        # Purchase Orders
        seed_purchase_header = BashOperator(
            task_id="seed_purchase_header",
            bash_command=generate_seeder_command(
                "purchasing/purchase_purchaseorderheader.py",
                "purchasing"
            ),
        )

        seed_purchase_detail = BashOperator(
            task_id="seed_purchase_detail",
            bash_command=generate_seeder_command(
                "purchasing/purchase_purchaseorderdetail.py",
                "purchasing"
            ),
        )

        seed_purchase_header >> seed_purchase_detail

        # Warehouse Transactions
        seed_warehouse_inventory = BashOperator(
            task_id="seed_warehouse_inventory",
            bash_command=generate_seeder_command(
                "warehouse/product_inventory.py",
                "warehouse"
            ),
        )

        seed_transaction_history = BashOperator(
            task_id="seed_transaction_history",
            bash_command=generate_seeder_command(
                "warehouse/transaction_history.py",
                "warehouse"
            ),
        )

    # ==================== ANALYTICS (Tabelas Derivadas) ====================
    # Dependem dos fatos

    with TaskGroup(group_id="seed_analytics") as seed_analytics:

        # Marketing Performance
        seed_marketing_campaign = BashOperator(
            task_id="seed_marketing_campaign",
            bash_command=generate_seeder_command(
                "marketing/marketing_campaign.py",
                "marketing"
            ),
        )

        seed_marketing_ads = BashOperator(
            task_id="seed_marketing_ads",
            bash_command=generate_seeder_command(
                "marketing/marketing_ads.py",
                "marketing"
            ),
        )

        seed_marketing_performance = BashOperator(
            task_id="seed_marketing_performance",
            bash_command=generate_seeder_command(
                "marketing/marketing_daily_performance.py",
                "marketing"
            ),
        )

        seed_marketing_campaign >> seed_marketing_ads >> seed_marketing_performance

    # Task final: Log de conclusão
    log_end = BashOperator(
        task_id="log_completion",
        bash_command="""
        echo "✅ DAG finalizada com sucesso!"
        echo "   📊 Todos os seeders executados com volumes randômicos"
        echo "   🎲 Próxima execução terá volumes diferentes"
        """
    )

    # ==================== DEPENDÊNCIAS ====================
    log_start >> seed_dimensions >> seed_facts >> seed_analytics >> log_end


# ==================== DOCUMENTAÇÃO DA DAG ====================

dag.doc_md = """
# DAG de Seeders com Randomização Dinâmica

Esta DAG executa todos os seeders do projeto com volumes e chaos rates
**dinâmicos e imprevisíveis** a cada execução, simulando cenários
realistas de engenharia de dados.

## Características

### 🎲 Randomização Total
- **Volume**: sorteado dentro de ranges configurados por schema
- **Chaos rate**: sorteado entre 1% e 10% (ou 0% para schemas críticos)
- **Imprevisível**: cada execução gera volumes diferentes

### 🔀 Execução Paralela
- **Dimensões**: executam em paralelo (sem dependências)
- **Fatos**: aguardam dimensões (integridade referencial)
- **Analytics**: aguardam fatos (dados derivados)

### 📊 Ranges por Schema

| Schema | Volume Mínimo | Volume Máximo | Chaos Min | Chaos Max |
|--------|---------------|---------------|-----------|-----------|
| humanresources | 50 | 100 | 0% | 0% |
| product | 500 | 1.500 | 2% | 5% |
| person | 10k | 5kk | 3% | 10% |
| sales | 100k | 50kk | 1% | 10% |
| purchasing | 50k | 50kk | 1% | 10% |
| warehouse | 100k | 50kk | 2% | 10% |
| marketing | 1k | 500k | 1% | 8% |

## Uso no Airflow

### 1. Execução Manual (via UI)
- Trigger DAG: clique em "Trigger DAG"
- Volumes serão sorteados automaticamente

### 2. Execução Agendada
- Schedule: diariamente às 02:00
- Catchup: desabilitado (não executa datas passadas)

### 3. Customização de Ranges
Para ajustar ranges, editar `SCHEMA_RANGES` no código da DAG.

## Monitoramento

### Logs
- Cada task gera logs detalhados com:
  - Ranges configurados
  - Valores sorteados
  - Progresso de inserção
  - Anomalias injetadas

### Alertas
- Email em caso de falha
- Retries configurados (2x com 5min de intervalo)

## Exemplos de Execução

### Execução 1 (dia normal):
- Sales: 150k registros, chaos 3%
- Warehouse: 200k registros, chaos 5%

### Execução 2 (Black Friday simulada):
- Sales: 35kk registros, chaos 8%
- Warehouse: 40kk registros, chaos 9%

### Execução 3 (dia de baixa):
- Sales: 100k registros, chaos 1%
- Warehouse: 150k registros, chaos 2%

## Próximos Passos

1. **Ajustar ranges** conforme política do projeto
2. **Configurar alertas** (Slack, PagerDuty, etc.)
3. **Integrar com dbt** para pipelines de limpeza
4. **Adicionar data quality checks** pós-seeding

## Contato

Para dúvidas ou ajustes, contatar:
- Time de Engenharia de Dados
- Email: data-team@company.com
"""
