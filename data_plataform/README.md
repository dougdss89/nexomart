# Data Platform — NexoMart

Plataforma de dados open-source construída do zero para o case **NexoMart** — um marketplace brasileiro multi-canal que processa entre 100 mil e 50 milhões de pedidos por ciclo. O objetivo é separar a camada transacional (OLTP) da analítica (OLAP), sem depender de nenhuma cloud proprietária.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────┐
│                     INGESTÃO & API                        │
│  FastAPI (async) + SQLAlchemy 2.0 + asyncpg + Pydantic   │
├──────────────────────────────────────────────────────────┤
│                    BANCO OLTP                             │
│  PostgreSQL 16 + pgvector + JSONB + Particionamento       │
├──────────────────────────────────────────────────────────┤
│                   ORQUESTRAÇÃO                            │
│           Apache Airflow 2.x (DAGs + BashOperator)        │
├──────────────────────────────────────────────────────────┤
│              PROCESSAMENTO DISTRIBUÍDO                    │
│         Apache Spark 3.x + Structured Streaming           │
├──────────────────────────────────────────────────────────┤
│              ARMAZENAMENTO ANALÍTICO                      │
│    Apache Iceberg + Nessie (versionamento) + MinIO (S3)   │
├──────────────────────────────────────────────────────────┤
│               FEDERAÇÃO DE CONSULTAS                      │
│                     Apache Trino                          │
└──────────────────────────────────────────────────────────┘
```

---

## Domínios de Negócio

| Schema | Domínio | Descrição |
|---|---|---|
| `product` | Produto | Catálogo, modelos, categorias, busca semântica via pgvector |
| `person` | Pessoa | Clientes, vendedores, endereços, contatos |
| `sales` | Vendas | Pedidos, itens (JSONB multi-seller), devoluções, pagamentos |
| `marketing` | Marketing | Campanhas, anúncios, performance por canal (4 níveis) |
| `humanresources` | RH | Funcionários, departamentos, histórico de cargos (SCD Type 2) |
| `purchasing` | Compras | Ordens de compra, fornecedores, recebimento |
| `warehouse` | Estoque | Inventário, transferências, histórico de movimentações |

---

## Estrutura do Repositório

```
data_plataform/
├── dbapi/                  # API de ingestão (FastAPI)
│   └── api/
│       ├── models/         # SQLAlchemy models por domínio
│       ├── schemas/        # Pydantic schemas
│       ├── repositories/   # Camada de acesso a dados (bulk insert)
│       ├── services/       # Lógica de negócio
│       ├── routers/        # Endpoints FastAPI
│       ├── seeder/         # Geradores de dados com chaos engineering
│       └── samples/        # Dados de referência por domínio
├── postgres/               # PostgreSQL 16 + init scripts
├── minio/                  # Object storage S3-compatible
├── airflow/                # Orquestração (DAGs + conexões)
├── spark/                  # Cluster Spark (master + workers + jobs)
├── hive/                   # Hive Metastore
├── trino/                  # Federação de consultas SQL
├── unitycatalog/           # Unity Catalog OSS (governança)
├── configurations/         # Variáveis de ambiente compartilhadas
├── docs/                   # Documentação do projeto
└── Makefile                # Comandos de operação da stack
```

---

## Pré-requisitos

- Docker + Docker Compose
- Make
- Python 3.12+ com [uv](https://github.com/astral-sh/uv)

---

## Início Rápido

### Primeira vez

```bash
# 1. Configure as credenciais (nunca commitadas)
cp configurations/secrets.env.example configurations/secrets.env
# edite secrets.env com os valores reais antes de continuar

# 2. Cria rede, volumes e builda as imagens
make setup

# 3. Sobe toda a stack (gera configs e inicia serviços)
make up
```

### Dia a dia

```bash
make up          # sobe tudo
make down        # para tudo (dados preservados)
make ps          # lista containers com status e portas
make logs s=trino-coordinator   # tail de logs de um container
make restart s=spark-master     # reinicia um container
make clean       # ⚠️  remove containers e volumes (apaga dados)
```

### Serviços individuais

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

---

## URLs dos Serviços

| Serviço | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | conforme `secrets.env` |
| Spark UI | http://localhost:9090 | — |
| Spark History | http://localhost:18080 | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | conforme `secrets.env` |
| Unity Catalog API | http://localhost:8182/api/2.1/unity-catalog | — |
| dbapi (FastAPI) | http://localhost:8001 | — |
| dbapi Docs | http://localhost:8001/docs | — |

---

## API de Ingestão (dbapi)

Arquitetura 3 camadas obrigatória em todos os domínios:

```
Router (FastAPI) → Service (regras de negócio) → Repository (bulk insert SQLAlchemy)
```

### Executar localmente

```bash
cd dbapi
uv run python api/app/main.py
```

### Rodar um seeder

```bash
uv run python dbapi/api/seeder/<dominio>/<arquivo>.py \
  --min-count 100000 \
  --max-count 500000 \
  --min-chaos 0.01 \
  --max-chaos 0.10
```

### Rodar os testes

```bash
cd dbapi
uv run python api/tests/test_product_services.py
```

---

## Chaos Engineering

Os seeders injetam entre **1% e 10%** de anomalias controladas a cada execução para simular cenários reais de qualidade de dados:

| Tipo | Exemplos |
|---|---|
| Duplicidade | Mesmo registro com variações de formatação |
| Dados sujos | CPF inválido, email sem TLD, telefone `0000000` |
| Fraudes | Compras geograficamente impossíveis, cadastros com CPF falso |
| Processos incompletos | Pedidos pagos mas nunca enviados, disputas de chargeback |
| Anomalias de estoque | Quantidades negativas, produtos vencidos em estoque ativo |

Documentação completa: [`dbapi/api/seeder/CHAOS_DOCUMENTATION.md`](dbapi/api/seeder/CHAOS_DOCUMENTATION.md)

---

## Modelo Dimensional

### Bus Matrix

| Fato | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| `fact_sales_order_item` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| `fact_inventory_snapshot` | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| `fact_marketing_performance` | ✓ | — | — | — | ✓ | — | — | — | — |
| `fact_purchase_order_line` | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| `fact_order_lifecycle` | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

---

## Prefixos de IDs Transacionais

| Domínio | Header | Linha |
|---|---|---|
| Vendas | `SO-XXXXXXXXX` | `SO-XXXXXXXXX-XX` |
| Compras | `PO-XXXXXXXXX` | `PO-XXXXXXXXX-XX` |
| Estoque | `TR-XXXXXXXXX` | `TR-XXXXXXXXX-XX` |
| Devoluções | `RO-XXXXXXXXX` | — |

---

## Documentação

| Arquivo | Descrição |
|---|---|
| [`docs/nexomart_requisitos.md`](docs/nexomart_requisitos.md) | Requisitos funcionais e não-funcionais do NexoMart |
| [`docs/CICLO_01_CRONOGRAMA.md`](docs/CICLO_01_CRONOGRAMA.md) | Cronograma de vídeos do Ciclo 1 e roteiros |
| [`docs/ENGINEERING_DEBATE_AND_ROADMAP.md`](docs/ENGINEERING_DEBATE_AND_ROADMAP.md) | Decisões de engenharia e roadmap técnico |
| [`docs/ESTRATEGIA_CONSOLIDADA_CONTEUDO.md`](docs/ESTRATEGIA_CONSOLIDADA_CONTEUDO.md) | Estratégia de conteúdo do canal |
| [`dbapi/walkthrough.md`](dbapi/walkthrough.md) | Walkthrough de implementação do domínio product |

---

## Stack Completa

| Componente | Tecnologia | Versão |
|---|---|---|
| API de Ingestão | FastAPI + asyncpg | 0.135 / 0.31 |
| ORM | SQLAlchemy | 2.0 |
| Validação | Pydantic | 2.12 |
| Banco OLTP | PostgreSQL | 16 |
| Busca Semântica | pgvector | — |
| Object Storage | MinIO | latest |
| Orquestração | Apache Airflow | 2.x |
| Processamento | Apache Spark | 3.x |
| Formato Analítico | Apache Iceberg | — |
| Versionamento de Dados | Nessie | — |
| Metastore | Hive Metastore | — |
| Federação SQL | Apache Trino | — |
| Governança | Unity Catalog OSS | — |
