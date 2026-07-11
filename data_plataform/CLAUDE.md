# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A data platform combining a FastAPI ingestion API (dbapi), Apache Airflow orchestration, Apache Spark distributed compute, Delta Lake tables (governed by Unity Catalog OSS, stored in MinIO), and Trino SQL federation — all connected via a shared Docker network (`hadoopcluster`).

## Commands

**Run API locally:**
```bash
cd dbapi
uv run python api/app/main.py
```

**Run a seeder:**
```bash
uv run python dbapi/api/seeder/<domain>/<file>.py --count 10000
```

**Run tests:**
```bash
cd dbapi
uv run python api/tests/test_product_services.py
```

**Start services — full stack (recommended):**
```bash
make setup        # primeira vez: cria rede/volumes + builda imagens
make up           # sobe toda a stack em ordem de dependência
make down         # para tudo (dados preservados)
make ps           # lista containers rodando
make logs s=spark-master        # tail de logs de um container
make restart s=trino-coordinator  # reinicia um container
make clean        # ⚠️  remove containers + volumes (apaga dados)
```

**Start services — individuais:**
```bash
make up-postgres
make up-minio
make up-hive
make up-unitycatalog
make up-spark
make up-trino
make up-airflow
make up-dbapi
```

**URLs após `make up`:**
| Serviço | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:9090 | — |
| Spark History | http://localhost:18080 | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | admin / admin |
| Unity Catalog API | http://localhost:8182/api/2.1/unity-catalog | — |

## Architecture

### 3-Layer API Architecture (dbapi)

All new models, schemas, and seeders **must** generate the full 3-layer stack:

1. **Repositories** (`dbapi/api/repositories/<domain>/`) — async data access only, no business logic. Use SQLAlchemy 2.0 bulk `insert(Model).values(matrix)`. **No for-loops.**
2. **Services** (`dbapi/api/services/<domain>/`) — business logic and orchestration. Import repositories, not raw SQL.
3. **Routers** (`dbapi/api/routers/<domain>_router.py`) — FastAPI endpoints with Pydantic schema validation.

### Models

All SQLAlchemy models in `dbapi/api/models/<domain>/` **must** inherit `AuditMixin` from `api/config/base.py` (adds `created_at`, `updated_at`, `is_active`, `row_guid`).

- Use `snake_case` for all PostgreSQL column names — never CamelCase
- Schema-qualified tables: `__table_args__ = ({"schema": "<domain>"})`
- Async driver: `postgresql+asyncpg://`

Domains: `product`, `sales`, `warehouse`, `purchasing`, `marketing`, `person`, `humanresources`

### Enums

Always inherit from `str`: `class Status(str, Enum)`. Enable `use_enum_values = True` in Pydantic `Config`.

### Pydantic

- Monetary fields: use `Decimal` with `Field(decimal_places=2, ge=0.0)` (`Numeric(19,4)` in DB)
- Cross-field validation: `@model_validator(mode='after')`

## Seeder Rules

Located in `dbapi/api/seeder/<domain>/`. Every seeder **must**:

- Implement `argparse` with `--count` argument (invoked by Airflow BashOperator via CLI)
- Fetch real foreign key IDs from the database — never generate orphaned records
- Use bulk insert: `insert(Model).values(matrix)` — **no for-loop inserts**
- Inject 5–10% chaos: null non-PK fields, malformed CPFs/CNPJs, "0000000" phones
- Print batch length and exceptions to stdout (for Airflow log visibility)
- Import sample/mock data from `dbapi/api/samples/<schema>.py`

For analytic fact seeders, derive revenue top-down: Impressions → CTR → Clicks → CPC → Cost → CVR → Conversions → AOV → Revenue.

### Data Volume Limits per Schema — Dynamic Ranges

**IMPORTANTE**: Os volumes são **dinâmicos e randômicos** a cada execução da DAG do Airflow. Os seeders devem sortear um valor dentro do range mínimo/máximo especificado.

| Schema | Tabelas Principais | Range Mínimo | Range Máximo | Observações |
|--------|-------------------|--------------|--------------|-------------|
| `sales` | Pedidos, Devoluções | 100.000 | **50 milhões** | Simular desde dias normais até picos (Black Friday, Natal) |
| `purchasing` | Ordens de Compra (Header + Detail) | 50.000 | **50 milhões** | Volume combinado header + detalhes |
| `warehouse` | Transaction History, Inventário | 100.000 | **50 milhões** | Movimentações de estoque com variação alta |
| `humanresources` | Employees, Departments, JobRoles | 50 | **100** | RH é dimensão pequena e estável |
| `product` | Products, Models, Categories | 500 | **1.500** | **Limite absoluto 100k**. Seeder deve verificar antes de inserir. |
| `person` | Customers, Addresses, Contacts | 10.000 | 5.000.000 | Baseado em volume de vendas (1:N) |
| `marketing` | Campaigns, Ads, Performance | 1.000 | 500.000 | Derivado de SKUs e períodos de campanha |

**Chaos Rate também é dinâmico**: Entre 1% e 10% sorteado aleatoriamente a cada execução.

#### Implementação nos Seeders

Todos os seeders devem aceitar **ranges** via argparse e sortear um valor dentro do range:

```python
import random

parser.add_argument("--min-count", type=int, default=100_000, help="Volume mínimo de registros")
parser.add_argument("--max-count", type=int, default=50_000_000, help="Volume máximo de registros")
parser.add_argument("--min-chaos", type=float, default=0.01, help="Chaos rate mínimo (1%)")
parser.add_argument("--max-chaos", type=float, default=0.10, help="Chaos rate máximo (10%)")

args = parser.parse_args()

# Sortear valores dinâmicos
count = random.randint(args.min_count, args.max_count)
chaos_rate = random.uniform(args.min_chaos, args.max_chaos)

print(f"🎲 Valores sorteados: {count:,} registros | Chaos: {chaos_rate:.2%}")
```

**Validação de Limite Absoluto** (para `product`):

```python
# Antes de inserir produtos
existing_count = await session.scalar(select(func.count(ProductModel.product_id)))

# Limite absoluto: 100k
ABSOLUTE_MAX = 100_000

if existing_count >= ABSOLUTE_MAX:
    print(f"⚠️ Limite absoluto de produtos atingido ({existing_count}/{ABSOLUTE_MAX}).")
    return

# Ajustar count sorteado para não ultrapassar limite
count = min(count, ABSOLUTE_MAX - existing_count)
print(f"✅ Inserindo {count:,} produtos (total final: {existing_count + count:,}/{ABSOLUTE_MAX:,})")
```

### Data Quality Rules — Chaos Engineering para Engenharia de Dados

**Objetivo**: Simular cenários reais de qualidade de dados para testar pipelines de limpeza, detecção de fraudes e governança.

Todos os seeders dos schemas `sales`, `purchasing`, `warehouse`, `person` devem injetar **entre 1% e 10%** das seguintes anomalias:

#### 1. Anomalias de Qualidade de Dados (Data Quality Issues)

- **Duplicidade** (1-5%): Registros duplicados com pequenas variações (troca de letra maiúscula, espaços extras, caracteres especiais)
  - Exemplos: `"João Silva"` vs `"JOÃO SILVA"` vs `"João  Silva "` (espaço duplo)
  - CPF duplicado com formatações diferentes: `"123.456.789-00"` vs `"12345678900"`

- **Dados Sujos** (2-10%): Campos com valores malformados que precisam de regexp/normalização
  - Telefones: `"0000000"`, `"11111111111"`, `"(00) 0000-0000"`
  - CEPs: `"00000-000"`, `"XXXXX-XXX"`, CEPs de estados diferentes do endereço
  - CPF/CNPJ: `"000.000.000-00"`, `"111.111.111-11"`, com dígitos verificadores errados
  - Emails: `"email@dominio"` (sem TLD), `"@dominio.com"` (sem usuário), `"email@@dominio.com"`
  - Nomes: `"João123"`, `"Maria###"`, `"Cliente Teste"`, `"XXXX"`, strings vazias com espaços `"   "`

- **Valores Acima do Normal** (1-3%): Outliers estatísticos (não fraudulentos, mas atípicos)
  - Pedidos com valores 5-10x acima da média (compras de luxo legítimas)
  - Quantidades muito grandes mas justificáveis (compra empresarial)
  - Tempo de entrega muito longo (importação, greve de transporte)

#### 2. Fraudes e Atividades Suspeitas (Fraud Detection Scenarios)

- **Fraudes em Compras** (1-2%):
  - Cliente com nome real mas CPF falso (`000.000.000-00`, sequências)
  - Cliente com nome falso mas endereço verdadeiro (entregas em endereços conhecidos)
  - Múltiplas compras com cartões diferentes no mesmo endereço em curto período
  - Cartões clonados: padrão de compras geograficamente impossível (compra em SP às 10h, em RJ às 10h05)
  - Compras de alto valor com dados cadastrais inconsistentes (email gratuito + endereço de luxo)

- **Cadastros Fraudulentos** (1-3%):
  - CPFs válidos matematicamente mas não existentes (gerados por algoritmo)
  - Múltiplos cadastros com mesmo telefone/email mas CPFs diferentes
  - Endereços falsos ou inexistentes (Rua X, número 99999, CEP inválido para o bairro)
  - Combinações impossíveis: nome brasileiro + telefone internacional não cadastrado

- **Processos de Compra Incompletos/Fantasmas** (2-5%):
  - Pedidos criados mas nunca pagos (`payment_status: "pending"` há mais de 30 dias)
  - Pedidos pagos mas nunca enviados (`shipping_status: null` ou `"pending"` indefinidamente)
  - Pedidos enviados mas nunca entregues (rastreamento parado há meses)
  - Pedidos com produtos que não existem no catálogo (referência a `product_id` deletado/inexistente)
  - Cliente recebeu produto mas há disputa de chargeback (`dispute_status: "open"`)

- **Fraudes em Purchasing** (1-2%):
  - Ordens de compra duplicadas para mesmo fornecedor (possível erro ou suborno)
  - Fornecedores fantasmas (CNPJ válido mas empresa sem atividade econômica)
  - Preços muito acima do mercado (superfaturamento)
  - Ordens aprovadas por funcionários sem autorização (employee_id sem permissão no sistema)
  - Entregas de produtos em endereços pessoais ao invés de warehouses

#### 3. Anomalias em Warehouse (Logística e Estoque)

- **Transferências Perdidas/Fantasmas** (2-5%):
  - Transferência iniciada (`status: "in_transit"`) mas nunca concluída (há mais de 90 dias)
  - Transferência concluída mas quantidade recebida diferente da enviada (quebra, roubo)
  - Transferência entre locais impossíveis (de warehouse A para A)
  - Transações sem rastreabilidade (`traceability_info: null` ou incompleto)

- **Inventário com Divergências** (3-8%):
  - Quantidade física diferente da quantidade no sistema (necessita contagem cíclica)
  - Produtos com custo negativo ou zero (erro de cadastro/sistema)
  - Produtos marcados como disponíveis mas sem localização física
  - Estoque negativo (overselling)
  - Movimentações sem ordem de origem (`reference_order_id: null` em transação de saída)

- **Problemas de Qualidade** (1-2%):
  - Produtos com `quality_check_status: "fail"` ou `"hold"` mas ainda assim enviados para clientes
  - Produtos com temperatura IoT fora do range permitido (cold chain quebrada)
  - Produtos vencidos ainda em estoque ativo

- **Valores Negativos** (2-4%):
  - Custos negativos (erro de sistema, necessita estorno)
  - Quantidades negativas em transações (bug de integração)
  - Valores de frete negativos (ajuste de sistema mal implementado)

#### 4. Implementação nos Seeders

Os seeders devem injetar essas anomalias de forma **randômica** mas **controlada**:

```python
def apply_data_quality_chaos(batch: List[Dict], chaos_rate: float = 0.05) -> List[Dict]:
    """
    Aplica chaos engineering (5% padrão) nos dados gerados.
    """
    num_chaos = int(len(batch) * chaos_rate)
    chaos_indices = random.sample(range(len(batch)), num_chaos)

    for idx in chaos_indices:
        chaos_type = random.choice([
            "duplicate", "dirty_data", "outlier", "fraud",
            "incomplete", "negative_values", "missing_refs"
        ])

        if chaos_type == "duplicate":
            # Duplicar registro com variação
            batch.append({**batch[idx], "customer_name": batch[idx]["customer_name"].upper()})

        elif chaos_type == "dirty_data":
            # Injetar CPF falso
            batch[idx]["cpf"] = "000.000.000-00"

        elif chaos_type == "fraud":
            # Cartão clonado: compras em locais impossíveis no mesmo dia
            batch[idx]["shipping_city"] = "São Paulo"
            batch[idx]["billing_city"] = "Rio de Janeiro"
            batch[idx]["order_date"] = datetime.now()
            batch[idx]["payment_date"] = datetime.now() - timedelta(minutes=5)

        # ... outros tipos de chaos

    return batch
```

**NOTA CRÍTICA**: Esses dados "sujos" são **intencionais** e devem ser **documentados em metadados** para permitir:
1. Testes de pipelines de limpeza (dbt, Spark)
2. Testes de detecção de fraudes (ML models)
3. Auditorias de qualidade de dados (Great Expectations, Soda)
4. Treinamento de analistas em cenários reais

Criar arquivo `dbapi/api/seeder/CHAOS_DOCUMENTATION.md` listando todos os tipos de anomalias injetadas por tabela.

## Transactional ID Prefixes

| Domain | Header | Line Item |
|---|---|---|
| Sales | `SO-XXXXXXXXX` | `SO-XXXXXXXXX-XX` |
| Purchasing | `PO-XXXXXXXXX` | `PO-XXXXXXXXX-XX` |
| Warehouse | `TR-XXXXXXXXX` | `TR-XXXXXXXXX-XX` |
| Returns | `RO-XXXXXXXXX` | — |

9-digit random numeric sequences for `X`.

## Workflow: Criando Novos Pipelines / Áreas Analíticas

Ao receber solicitação de criação de um novo pipeline (ex: Vendas, Logística), seguir **obrigatoriamente** esta hierarquia em ordem:

### Passo 1 — Model SQLAlchemy (`dbapi/api/models/<domain>/`)
1. Criar o model estendendo `Base` e `AuditMixin`.
2. Forçar `__tablename__` em minúsculo.
3. Índices JSONB: declarar com casting e mapear chaves `snake_case` nativas.

### Passo 2 — Schema Pydantic (`dbapi/api/schemas/<domain>/`)
1. Espelhar `1:1` as chaves do Model SQL.
2. Enums devem herdar de `str` com restrições rigorosas.
3. UUID e Booleans padrão.
4. `@model_validator(mode='after')` para datas ou lógicas de negócio.

### Passo 3 — Seeder Realístico (`dbapi/api/seeder/<domain>/`)
1. Se herda dados de outro pipeline, importar o array do seeder anterior (ex: `get_person_customer_seeders()`).
2. Cruzar com tabelas satélite em `dbapi/api/samples/`.
3. Construir o gerador injetando payload JSON bruto via `PipelineSchema(**payload).model_dump()` para blindar o dado.
4. Fatos numéricos (Time-Series): aplicar Matemática de Funil.
5. Entidades (Dimensões): aplicar Chaos Engineering (5–10% erros sintáticos controlados).

### Passo 4 — Tríade REST 3-Layer (Repository → Service → Router)
**Obrigatório** ao concluir o Seeder:
1. `_repository.py` em `dbapi/api/repositories/<domain>/` — `bulk_insert()` assíncrono via SQLAlchemy puro. **Zero for-loops.**
2. `_service.py` abstrato com precondições analíticas em `dbapi/api/services/<domain>/`.
3. Router em `dbapi/api/routers/<domain>_router.py` vinculado a `Depends(get_db)`.

### Passo 5 — Integração CLI / Airflow
1. Expor `argparse` no escopo `__main__` do Seeder (ex: `--count`, `--batch_size`) para disparo via `BashOperator`.
2. Confirmar que nenhum for-loop envia queries individuais — todas as operações em lote pela Tríade.
3. Testar no terminal e reportar ao usuário antes de declarar "concluído".

---

## Configuração de Infraestrutura — Zero Hardcode

**Regra de ouro**: nenhuma string de conexão (host, porta, nome de banco, credencial, endpoint de serviço) deve ser escrita literal em `docker-compose.yml`/config de serviço. Tudo vem de `configurations/secrets.env` (única fonte de verdade — nunca commitado, mas com `secrets.env.example` espelhando as chaves). Para adicionar um valor novo: declare a variável em `secrets.env` **e** em `secrets.env.example`, nunca hardcode um literal novo em nenhum arquivo de config.

Três mecanismos de substituição, dependendo do tipo de arquivo — usar o certo para cada um:

1. **Docker Compose** (`*/docker-compose.yml`): sintaxe nativa `${VAR}`. O `Makefile` já exporta `secrets.env` pro shell antes de qualquer `docker compose up` (target `up`/`up-*`), então `${VAR}` dentro do YAML resolve sozinho. Se o *container* também precisa enxergar a variável em runtime (não só na hora de montar o compose), adicione `env_file: ../configurations/secrets.env` ao serviço.
2. **Catálogos do Trino** (`trino/catalog/*.properties`): sintaxe própria do Trino `${ENV:VAR}` (não é `${VAR}` puro). Os serviços `trino-coordinator`/`trino-worker` já carregam `secrets.env` via `env_file`.
3. **Arquivos de config sem suporte nativo a variável** (Java `.properties` do Unity Catalog, `spark-defaults.conf`, XML do Hive): **nunca edite o arquivo final diretamente** — edite o `<arquivo>.template` correspondente (usa `${VAR}` estilo shell) e rode `make generate-confs` (ou já incluso em `make up`), que roda `envsubst` e gera o arquivo real. Editar o arquivo gerado é perdido no próximo `generate-confs`.

Arquivos de config sem template e sem variável (`hive/conf/hive-site.xml`, `nessie/application.properties` — este último nem está montado em nenhum container, é código morto) ficam de fora deste padrão por ora; não hardcode neles sem necessidade, mas também não presuma que aceitam `${VAR}` sem testar.

---

## Key Files

- `dbapi/api/config/base.py` — `AuditMixin` and `Base` declarative base
- `dbapi/api/config/settings.py` — env-based configuration
- `dbapi/api/database/database.py` — `AsyncSessionLocal` factory
- `dbapi/api/samples/` — mock data templates imported by seeders
- `configurations/.env` — shared env vars for PostgreSQL, MinIO, Spark, etc.
- `dbapi/walkthrough.md` — implementation walkthrough for the product domain (reference example)
