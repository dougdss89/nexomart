# NexoMart — Requisitos Funcionais e Não-Funcionais

## Contexto da Empresa

**NexoMart** é um marketplace brasileiro de médio porte com operação multi-canal (web, app, marketplace partners) que processa entre **100 mil e 50 milhões de pedidos por ciclo de dados** dependendo da sazonalidade (Black Friday, Natal, Dia das Mães).

A empresa opera com um banco OLTP PostgreSQL maduro, mas enfrenta um problema clássico: **o banco transacional não consegue responder às perguntas analíticas do negócio em tempo razoável** sem afetar a operação.

---

## Domínios de Negócio

| Domínio | Schema | Descrição |
|---|---|---|
| Produto | `product` | Catálogo de produtos, modelos, categorias, descrições em vetores |
| Pessoa | `person` | Clientes, vendedores, endereços, contatos |
| Vendas | `sales` | Pedidos, itens (JSONB), devoluções, pagamentos |
| Marketing | `marketing` | Campanhas, anúncios, performance por canal |
| RH | `humanresources` | Funcionários, departamentos, histórico de cargos (SCD Type 2) |
| Compras | `purchasing` | Ordens de compra, fornecedores, recebimento |
| Estoque | `warehouse` | Inventário, transferências, histórico de movimentações |

---

## Requisitos Funcionais

### RF-01 — Ingestão de Dados Operacionais

- O sistema deve ingerir dados transacionais de todos os 7 domínios via API REST assíncrona (FastAPI + asyncpg)
- A ingestão deve suportar bulk insert para volumes de até 50M registros por execução
- A API deve implementar arquitetura 3-layer: Repository → Service → Router
- Todos os modelos devem implementar auditoria automática (`created_at`, `updated_at`, `is_active`, `row_guid`)

### RF-02 — Orquestração de Pipelines

- O sistema deve orquestrar todos os pipelines via Apache Airflow com DAGs declarativas
- Os seeders devem ser acionados via `BashOperator` como scripts CLI com `argparse`
- Volumes de dados devem ser dinâmicos por execução (range `min/max` sorteado aleatoriamente)
- A taxa de chaos engineering deve variar entre 1% e 10% por execução

### RF-03 — Armazenamento Distribuído e Versionado

- Dados processados devem ser armazenados em tabelas Apache Iceberg no MinIO (object storage S3-compatible)
- O versionamento das tabelas deve ser gerenciado pelo Nessie (controle de branches de dados)
- O formato de armazenamento deve suportar time-travel queries e rollback de dados

### RF-04 — Processamento Distribuído

- Transformações analíticas devem ser executadas via Apache Spark
- O Spark deve ler do PostgreSQL (OLTP) e escrever em Iceberg (OLAP) via camadas Bronze / Silver / Gold
- O processamento deve suportar Structured Streaming para cenários near-real-time

### RF-05 — Federação de Consultas

- Analistas devem consultar dados via SQL padrão usando Apache Trino
- O Trino deve federar dados do Iceberg (MinIO/Nessie), PostgreSQL e futuras fontes
- Queries analíticas devem retornar resultados em menos de 10s para volumes de 50M registros

### RF-06 — Qualidade de Dados e Chaos Engineering

- O sistema deve injetar anomalias controladas (duplicatas, dados sujos, fraudes simuladas) para testar pipelines de qualidade
- As anomalias devem ser documentadas em metadados para uso em testes de ML e auditoria
- O sistema deve suportar cenários de detecção de fraude, chargeback e dados incompletos

### RF-07 — Capacidades AI-Ready

- O domínio `product` deve suportar busca semântica via pgvector (embeddings de descrições)
- O sistema deve estar preparado para integração com modelos de linguagem para geração de insights
- A modelagem deve suportar futuras features de recomendação de produtos

---

## Requisitos Não-Funcionais

| Requisito | Meta |
|---|---|
| Throughput de ingestão | 50M registros/execução |
| Latência analítica | < 10s para queries sobre 50M registros |
| Disponibilidade OLTP | 99.9% |
| Retenção de dados | Indefinida (Iceberg + MinIO com versionamento) |
| Rastreabilidade | 100% das operações com audit trail |
| Portabilidade | 100% open-source, zero custo de cloud proprietária |

---

## Stack Tecnológica

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

## Modelo Dimensional — Bus Matrix

| Fato | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| `fact_sales_order_item` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| `fact_inventory_snapshot` | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| `fact_marketing_performance` | ✓ | — | — | — | ✓ | — | — | — | — |
| `fact_purchase_order_line` | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| `fact_order_lifecycle` | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

### Grain de Cada Fato

| Fato | Grain (1 linha = 1 ...) | Fonte OLTP |
|------|------------------------|------------|
| `fact_sales_order_item` | Item dentro de um pedido (unnest do JSONB) | `sales.salesorder.order_items` |
| `fact_inventory_snapshot` | Produto × Warehouse × Dia | `warehouse.productinventory` |
| `fact_marketing_performance` | Anúncio × Dia | `marketing.dailyperformance` |
| `fact_purchase_order_line` | Linha de ordem de compra | `purchasing.purchaseorderdetail` |
| `fact_order_lifecycle` | Pedido × Evento de status | `sales.ordereventlog` |

---

*Documento de referência para o case NexoMart — Data Platform do Zero*
