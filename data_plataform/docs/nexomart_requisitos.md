# NexoMart — Functional and Non-Functional Requirements

## Company Context

**NexoMart** is a mid-size Brazilian marketplace with multi-channel operations (web, app, marketplace partners) that processes between **100 thousand and 50 million orders per data cycle** depending on seasonality (Black Friday, Christmas, Mother's Day).

The company runs on a mature PostgreSQL OLTP database, but faces a classic problem: **the transactional database can't answer the business's analytical questions in reasonable time** without impacting operations.

---

## Business Domains

| Domain | Schema | Description |
|---|---|---|
| Product | `product` | Product catalog, models, categories, vector descriptions |
| Person | `person` | Customers, sellers, addresses, contacts |
| Sales | `sales` | Orders, items (JSONB), returns, payments |
| Marketing | `marketing` | Campaigns, ads, performance by channel |
| HR | `humanresources` | Employees, departments, job role history (SCD Type 2) |
| Purchasing | `purchasing` | Purchase orders, suppliers, receiving |
| Warehouse | `warehouse` | Inventory, transfers, movement history |

---

## Functional Requirements

### FR-01 — Operational Data Ingestion

- The system must ingest transactional data from all 7 domains via an async REST API (FastAPI + asyncpg)
- Ingestion must support bulk insert for volumes of up to 50M records per run
- The API must implement a 3-layer architecture: Repository → Service → Router
- All models must implement automatic audit fields (`created_at`, `updated_at`, `is_active`, `row_guid`)

### FR-02 — Pipeline Orchestration

- The system must orchestrate all pipelines via Apache Airflow with declarative DAGs
- Seeders must be triggered via `BashOperator` as CLI scripts with `argparse`
- Data volumes must be dynamic per run (`min/max` range randomly drawn)
- The chaos engineering rate must vary between 1% and 10% per run

### FR-03 — Distributed, Versioned Storage

- Processed data must be stored in Apache Iceberg tables on MinIO (S3-compatible object storage)
- Table versioning must be managed by Nessie (data branch control)
- The storage format must support time-travel queries and data rollback

### FR-04 — Distributed Processing

- Analytical transformations must run via Apache Spark
- Spark must read from PostgreSQL (OLTP) and write to Iceberg (OLAP) through Bronze / Silver / Gold layers
- Processing must support Structured Streaming for near-real-time scenarios

### FR-05 — Query Federation

- Analysts must be able to query data via standard SQL using Apache Trino
- Trino must federate data across Iceberg (MinIO/Nessie), PostgreSQL, and future sources
- Analytical queries must return results in under 10s for volumes of 50M records

### FR-06 — Data Quality and Chaos Engineering

- The system must inject controlled anomalies (duplicates, dirty data, simulated fraud) to test quality pipelines
- Anomalies must be documented in metadata for use in ML testing and auditing
- The system must support fraud detection, chargeback, and incomplete-data scenarios

### FR-07 — AI-Ready Capabilities

- The `product` domain must support semantic search via pgvector (description embeddings)
- The system must be prepared for integration with language models for insight generation
- The data model must support future product-recommendation features

---

## Non-Functional Requirements

| Requirement | Target |
|---|---|
| Ingestion throughput | 50M records/run |
| Analytical latency | < 10s for queries over 50M records |
| OLTP availability | 99.9% |
| Data retention | Indefinite (Iceberg + MinIO with versioning) |
| Traceability | 100% of operations with audit trail |
| Portability | 100% open-source, zero proprietary cloud cost |

---

## Technology Stack

```
┌──────────────────────────────────────────────────────────┐
│                   INGESTION & API                         │
│  FastAPI (async) + SQLAlchemy 2.0 + asyncpg + Pydantic   │
├──────────────────────────────────────────────────────────┤
│                   OLTP DATABASE                            │
│  PostgreSQL 16 + pgvector + JSONB + Partitioning           │
├──────────────────────────────────────────────────────────┤
│                   ORCHESTRATION                            │
│           Apache Airflow 2.x (DAGs + BashOperator)        │
├──────────────────────────────────────────────────────────┤
│              DISTRIBUTED PROCESSING                        │
│         Apache Spark 3.x + Structured Streaming           │
├──────────────────────────────────────────────────────────┤
│              ANALYTICAL STORAGE                            │
│    Apache Iceberg + Nessie (versioning) + MinIO (S3)      │
├──────────────────────────────────────────────────────────┤
│                  QUERY FEDERATION                          │
│                     Apache Trino                           │
└──────────────────────────────────────────────────────────┘
```

---

## Dimensional Model — Bus Matrix

| Fact | date | customer | product | seller | channel | warehouse | employee | vendor | special_offer |
|------|:----:|:--------:|:-------:|:------:|:-------:|:---------:|:--------:|:------:|:-------------:|
| `fact_sales_order_item` | ✓ | ✓ | ✓ | ✓ | ✓ | — | — | — | ✓ |
| `fact_inventory_snapshot` | ✓ | — | ✓ | — | — | ✓ | — | — | — |
| `fact_marketing_performance` | ✓ | — | — | — | ✓ | — | — | — | — |
| `fact_purchase_order_line` | ✓ | — | ✓ | — | — | — | ✓ | ✓ | — |
| `fact_order_lifecycle` | ✓ | ✓ | — | ✓ | ✓ | — | — | — | — |

### Grain of Each Fact

| Fact | Grain (1 row = 1 ...) | OLTP Source |
|------|------------------------|------------|
| `fact_sales_order_item` | Item within an order (unnest of JSONB) | `sales.salesorder.order_items` |
| `fact_inventory_snapshot` | Product × Warehouse × Day | `warehouse.productinventory` |
| `fact_marketing_performance` | Ad × Day | `marketing.dailyperformance` |
| `fact_purchase_order_line` | Purchase order line | `purchasing.purchaseorderdetail` |
| `fact_order_lifecycle` | Order × Status event | `sales.ordereventlog` |

---

*Reference document for the NexoMart case — Data Platform from Scratch*
