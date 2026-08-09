# Data Platform — Engineering Debate & Implementation Roadmap

> **Context**: Collaborative architectural review simulating a debate between three senior engineers to identify gaps, suggest improvements, and prioritize an evolution roadmap. Intended for a personal data engineering portfolio.

> **Documented hardware constraint**: The `dbapi` (FastAPI) **does not run as a persistent online service** in this portfolio. All load is triggered by Airflow via **BashOperator**, which invokes the seeders as CLI scripts (`uv run python seeder.py --min-count X --max-count Y`). The 3-layer architecture (Repository → Service → Router) exists in the code and correctly demonstrates the pattern — but the runtime is batch/CLI, not HTTP. Roadmap suggestions that depend on an online API (JWT Auth, Rate Limiting, Cursor Pagination, Redis Cache-Aside for user reads) are **aspirational** — correct for production, but not applicable to the current setup due to hardware limitations.

---

## Participants

| Role | Name | Specialty |
|---|---|---|
| Senior Data Engineer | **Rafael** | Streaming, Kafka, event-driven systems |
| Senior Data Engineer | **Beatriz** | Data quality, dbt, platform architecture |
| Principal Data Architect | **Lucas** | Distributed systems, performance, portfolio design |

---

## Part 1 — Technical Debate

### Round 1: Ingestion and Streaming (Kafka)

---

**Rafael:** I'll get straight to the point. The API is taking on a **synchronous ingestion** role for volumes that can reach 50 million records per run. The endpoint receives the payload, passes it through the service, and waits for the bulk insert to finish before responding. That's a design bottleneck, not an implementation one.

The problem isn't `insert(Model).values(matrix)` — that pattern is correct. The problem is that the API is simultaneously the **producer**, the **processor**, and the **writer**. If Airflow calls the seeder directly via BashOperator while three DAGs run in parallel for `sales`, `purchasing`, and `warehouse` at the same time, we get connection contention on PostgreSQL and no backpressure.

**Proposal**: Introduce Apache Kafka as a buffer layer between the seeder and the database. The seeder publishes events to the topic. A consumer group (async workers) consumes them and performs the bulk insert. Airflow then orchestrates producers, not direct writers.

```
Seeder (Producer) → Kafka Topic → Consumer Group → PostgreSQL Bulk Insert
                                                  → Iceberg (via Spark Structured Streaming)
```

---

**Beatriz:** I agree with the Kafka rationale, but I need to qualify it: for a portfolio, implementing Kafka without a **Schema Registry** and **Data Contracts** is an anti-pattern that will come back to bite you. I've seen projects where the producer changed the event schema and the consumer broke silently for three days.

The project already has `apache-airflow-providers-apache-kafka==1.12.0` as a dependency — the provider is already declared, but there's no DAG using it. That's an explicit gap.

Proposal to complement Rafael's: use **Apache Avro + Confluent Schema Registry** (or Karapace, which is open source) for schema contracts between producers and consumers. Each domain has its own topic with a versioned schema:

```
kafka-topics:
  - data_platform.sales.orders.v1
  - data_platform.person.customers.v1
  - data_platform.warehouse.transactions.v1
  - data_platform.dlq.poison_pills  (dead letter queue)
```

---

**Lucas:** I'll bring the portfolio perspective. Kafka with Schema Registry is correct, but we need to implement the **Outbox Pattern** in the API to guarantee exactly-once delivery in case of failure between the PostgreSQL insert and the Kafka publish. Without it, the portfolio shows the candidate knows Kafka, but doesn't know the problems Kafka introduces.

The Outbox Pattern works like this:

1. The service inserts the entity **and** inserts into `outbox` within a single transaction.
2. A separate process (Debezium CDC or an Airflow job) reads the `outbox` table and publishes to Kafka.
3. After Kafka confirms, the record is marked as `processed`.

```sql
-- Outbox table (per schema/domain)
CREATE TABLE sales.outbox (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate   VARCHAR(100),       -- 'SalesOrder', 'SalesReturn'
    event_type  VARCHAR(100),       -- 'OrderCreated', 'OrderCancelled'
    payload     JSONB,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    processed   BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMPTZ
);
```

This turns the project from "API that uses Kafka" into "event-driven system with consistency guarantees" — a huge difference in an interview.

---

### Round 2: Observability and Data Quality

---

**Beatriz:** Here's my biggest issue with the current project: **chaos engineering** is excellent — `chaos_engineering.py` with 35+ strategies is impressive for a portfolio. But there's nothing that **detects** the chaos that was injected.

The complete data engineering cycle is:
```
Ingest → Transform → Validate → Monitor → Alert → Remediate
```

The project covers `Ingest` well and `Transform` partially. But `Validate`, `Monitor`, `Alert`, and `Remediate` are missing.

**Concrete proposal**:

1. **dbt** for silver → gold transformations with native `dbt tests` (`not_null`, `unique`, `accepted_values`, `relationships`)
2. **Great Expectations** or **Soda** integrated into the DAGs for validation after each load
3. **Apache Superset** or **Metabase** for data quality dashboards
4. `data_quality_log` table in PostgreSQL for a history of detected anomalies

The big showcase here is: "I intentionally injected dirty data into bronze and built a pipeline that automatically detects, quarantines, and reports it in silver."

---

**Rafael:** Data validation is necessary, but platform observability is equally critical. Today nothing answers: "How long did the bulk insert of 5M records take? What was the throughput? Did it degrade over time?"

**Proposal**: OpenTelemetry as a unified observability layer:

```python
# dbapi/api/config/telemetry.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

tracer = trace.get_tracer("data_platform.dbapi")

# Usage in services
async def bulk_insert_orders(session, data):
    with tracer.start_as_current_span("sales.orders.bulk_insert") as span:
        span.set_attribute("record_count", len(data))
        span.set_attribute("chaos_rate", chaos_rate)
        # ... insert logic
        span.set_attribute("duration_ms", elapsed)
```

Observability stack for the portfolio:
- **Traces**: OpenTelemetry → Jaeger or Tempo
- **Metrics**: Prometheus + Grafana (scraping the API and Kafka workers)
- **Logs**: Structured JSON → Loki or OpenSearch
- **Alerts**: AlertManager for DAG SLAs

---

**Lucas:** Great, but I want to connect the two points. The project has `apache-airflow-providers-openlineage==1.9` installed and it **isn't being used**. OpenLineage is the data lineage standard that Marquez/OpenMetadata consumes.

If the project instruments OpenLineage in the DAGs, the portfolio demonstrates end-to-end data lineage traceability:

```
PostgreSQL (source) → Spark Job (transform) → Delta Lake (sink)
                   OpenLineage event →  Marquez UI (lineage graph)
```

That's data-company-grade sophistication. Candidates who know lineage automatically clear the mid-level engineer filter and get evaluated as senior.

---

### Round 3: Performance and Design Patterns

---

**Lucas:** Now I'll be direct about design patterns. The 3-layer architecture (Repository → Service → Router) is correct, but it's implemented as **pure CRUD**. For a data engineering portfolio, we need to show more sophisticated patterns.

**CQRS (Command Query Responsibility Segregation)** is the most relevant one here:

```
Write Path (Commands): FastAPI → Service → Repository → PostgreSQL → Kafka → Delta Lake
Read  Path (Queries):  FastAPI → Query Service → Trino (federated SQL over Delta Lake)
```

Why this matters: today, if someone wants to query 50M `sales` records, the query goes straight to the OLTP PostgreSQL. With CQRS, analytical reads go to Trino over Delta Lake (OLAP) and writes go to PostgreSQL (OLTP). The project already has Trino, but no API endpoint uses it.

---

**Rafael:** I agree with CQRS, but I want to add **partial Event Sourcing** for the transactional domains. Instead of storing only the current state of an order, we store the sequence of events:

```
OrderCreated → OrderPaymentConfirmed → OrderShipped → OrderDelivered
```

Each event is immutable. The current state is the projection (fold) of the events. This is natural with Kafka — the Kafka log **is** the event store.

Portfolio benefit: it demonstrates understanding of eventual consistency, projections, and event replay — concepts that come up in senior-level interviews at companies like iFood, Nubank, Mercado Livre.

---

**Beatriz:** I'll bring up a more pragmatic performance point nobody's mentioned: **there's no pagination or cursor-based pagination in the routers**.

```python
# How it stands today (assumed)
@router.get("/orders")
async def list_orders(db: AsyncSession = Depends(get_db)):
    return await order_service.get_all(db)  # SELECT * — no limit

# How it should be
@router.get("/orders")
async def list_orders(
    cursor: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db)
):
    return await order_service.get_page(db, cursor=cursor, limit=limit)
```

Cursor-based pagination is more efficient than offset pagination for large tables (with offset, the database reads and discards the first N records). With 50M records in `sales`, this is critical.

Second point: **Redis isn't being used for caching**, despite being configured in `.env`. Cache-Aside pattern for frequent dimension queries (products, customers, locations):

```python
async def get_product(product_id: UUID) -> Product:
    cache_key = f"product:{product_id}"
    cached = await redis.get(cache_key)
    if cached:
        return Product.model_validate_json(cached)
    product = await product_repo.get_by_id(db, product_id)
    await redis.setex(cache_key, 3600, product.model_dump_json())
    return product
```

---

### Round 4: Security and CI/CD

---

**Beatriz:** Security is the most critical point for a portfolio. The `minioadmin:minioadmin` and `admin:admin` credentials in `.env` are acceptable for local development, but the portfolio needs to show that the candidate knows what **not to do in production**.

**What to add**:
1. **Makefile** or setup script that generates `.env` from `.env.example` (never commit the real `.env`)
2. **GitHub Actions** with `trufflehog` or `detect-secrets` to block commits containing credentials
3. **Docker Secrets** or **HashiCorp Vault** (dev mode) for runtime secrets
4. **JWT Authentication** on the API routers with FastAPI Security

```python
# dbapi/api/config/auth.py
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

async def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(status_code=401)
```

---

**Rafael:** CI/CD is missing, and it's one of the first things recruiters look at on GitHub. The project needs at least:

```yaml
# .github/workflows/ci.yml
name: CI Pipeline
on: [push, pull_request]

jobs:
  lint:
    - ruff check dbapi/
    - mypy dbapi/api/

  test:
    - pytest dbapi/api/tests/ --cov=api --cov-report=xml

  docker-build:
    - docker build -t dbapi:${{ github.sha }} dbapi/

  integration:
    services:
      postgres: postgres:16
    - pytest dbapi/api/tests/integration/
```

On top of that, **pre-commit hooks** with `ruff`, `mypy`, `detect-secrets`, and `hadolint` (Dockerfile linter).

---

**Lucas:** To close out the debate, I want to talk about what sets this portfolio apart from 95% of GitHub projects:

1. **README with visual architecture** — `diagram.py` exists but the image doesn't show up in the README
2. **Demo video / GIF** — show Airflow running, Trino querying, chaos engineering in action
3. **Unified Makefile** — `make start`, `make seed`, `make test`, `make stop` to make it easy to reproduce
4. **Separate ARCHITECTURE.md** with architectural decisions (ADRs — Architecture Decision Records)
5. **Badges in the README** — CI passing, coverage %, Python version, license

What makes a data engineering portfolio stand out isn't just technical complexity — it's **communicating that technical complexity**. A technical recruiter spends 3 minutes on the README before deciding whether to keep going.

---

## Part 2 — Implementation Roadmap

### Prioritization by Impact × Effort

> **Mode legend**: `CLI` = applies to the current mode (BashOperator); `ONLINE` = requires the API running as a service (aspirational); `INFRA` = infrastructure component independent of the API's mode.

| # | Item | Mode | Portfolio Impact | Effort | Priority |
|---|------|:----:|:-----------------:|:-------:|:----------:|
| 1 | README with architecture and badges | INFRA | High | Low | **P0** |
| 2 | Unified Makefile (`make start/stop/seed/test`) | INFRA | High | Low | **P0** |
| 3 | `.env.example` + removing credentials from git | INFRA | High | Low | **P0** |
| 4 | GitHub Actions CI (lint + test + build) | INFRA | High | Medium | **P0** |
| 5 | Kafka (producer in the CLI seeder) + consumer group | CLI | High | High | **P1** |
| 6 | Schema Registry (Karapace) + Avro schemas | CLI | High | High | **P1** |
| 7 | dbt project (silver → gold transforms) | INFRA | High | High | **P1** |
| 8 | OpenLineage in the DAGs + Marquez UI | INFRA | High | Medium | **P1** |
| 9 | Alembic migrations (currently missing) | CLI | Medium | Medium | **P1** |
| 10 | pytest integration with AsyncSession fixtures | CLI | Medium | Medium | **P1** |
| 11 | Outbox Pattern (table + Debezium CDC) | CLI | High | High | **P2** |
| 12 | Great Expectations integrated into the DAGs | INFRA | Medium | Medium | **P2** |
| 13 | Prometheus + Grafana stack | INFRA | Medium | Medium | **P2** |
| 14 | OpenTelemetry in the CLI seeders (no HTTP) | CLI | Medium | Medium | **P2** |
| 15 | CQRS Read Path via Trino (query DAG) | INFRA | Medium | High | **P2** |
| 16 | Cursor-based pagination in the routers | ONLINE | Low | Medium | **P3** |
| 17 | Redis Cache-Aside for dimensions | ONLINE | Low | Medium | **P3** |
| 18 | JWT Authentication in the routers | ONLINE | Low | Medium | **P3** |
| 19 | Event Sourcing for the `sales` domain | CLI | Medium | High | **P3** |
| 20 | HashiCorp Vault (dev mode) | INFRA | Low | High | **P3** |

---

## Part 3 — Implementation Specs

### SPEC-01: Apache Kafka Integration

**Objective**: Decouple data production and consumption, add backpressure, and support Structured Streaming via Spark.

**Architecture**:

```
                          ┌─────────────────────────────────────────────┐
                          │              KAFKA CLUSTER                   │
                          │                                              │
  Seeder/API              │  ┌─────────────────────────────────────────┐ │
  (Producer)  ──Avro──►  │  │  data_platform.sales.orders.v1          │ │
                          │  │  data_platform.person.customers.v1      │ │
  Airflow DAG             │  │  data_platform.warehouse.transactions.v1│ │
  (Kafka Operator) ──►   │  │  data_platform.dlq.poison_pills         │ │
                          │  └─────────────────────────────────────────┘ │
                          └────────────────┬────────────────────────────┘
                                           │
                   ┌───────────────────────┼────────────────────────┐
                   │                       │                        │
                   ▼                       ▼                        ▼
           PostgreSQL Consumer    Spark Structured          Delta Lake Sink
           (bulk insert)          Streaming Job             (bronze layer)
```

**Files to create**:

```
kafka/
├── docker-compose.yml          # Kafka + Zookeeper + Karapace (Schema Registry)
├── config/
│   ├── server.properties       # Kafka broker config
│   └── karapace.config.json    # Schema registry config
dbapi/api/
├── kafka/
│   ├── producer.py             # KafkaProducer with Avro serialization
│   ├── consumer.py             # KafkaConsumer base class with bulk insert
│   └── schemas/                # Avro schemas per domain
│       ├── sales_order.avsc
│       ├── person_customer.avsc
│       └── warehouse_transaction.avsc
```

**`kafka/docker-compose.yml`**:

```yaml
version: "3.8"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    networks:
      - hadoopcluster

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_RETENTION_HOURS: 168  # 7 days
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
    networks:
      - hadoopcluster

  karapace:
    image: ghcr.io/aiven-open/karapace:latest  # Open-source Schema Registry
    ports:
      - "8081:8081"
    environment:
      KARAPACE_ADVERTISED_HOSTNAME: karapace
      KARAPACE_BOOTSTRAP_URI: kafka:29092
      KARAPACE_PORT: 8081
      KARAPACE_HOST: 0.0.0.0
      KARAPACE_CLIENT_ID: karapace
      KARAPACE_GROUP_ID: karapace-registry
      KARAPACE_MASTER_ELIGIBILITY: "true"
      KARAPACE_TOPIC_NAME: _schemas
      KARAPACE_COMPATIBILITY: FULL_TRANSITIVE  # Strict backward + forward compatibility
    depends_on: [kafka]
    networks:
      - hadoopcluster

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8082:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: data-platform
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://karapace:8081
    networks:
      - hadoopcluster

networks:
  hadoopcluster:
    external: true
```

**`dbapi/api/kafka/producer.py`**:

```python
import json
import logging
from typing import Any, Dict, List
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from api.config.settings import settings

logger = logging.getLogger(__name__)


class DataPlatformProducer:
    """
    Kafka producer with Avro serialization and Schema Registry.
    Ensures every published event conforms to the registered schema.
    """

    def __init__(self, topic: str, schema_str: str):
        schema_registry_conf = {"url": settings.KAFKA_SCHEMA_REGISTRY_URL}
        self.schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        self.avro_serializer = AvroSerializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=schema_str,
        )

        self.producer = Producer({"bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS})
        self.topic = topic

    def publish_batch(self, records: List[Dict[str, Any]]) -> int:
        published = 0
        for record in records:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=str(record.get("id", "")),
                    value=self.avro_serializer(
                        record,
                        SerializationContext(self.topic, MessageField.VALUE)
                    ),
                    on_delivery=self._delivery_report,
                )
                published += 1
            except Exception as exc:
                logger.error("Failed to publish record to %s: %s", self.topic, exc)

        self.producer.flush()
        logger.info("Published %d/%d records to topic %s", published, len(records), self.topic)
        return published

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            logger.error("Message delivery failed: %s", err)
```

**`dbapi/api/kafka/schemas/sales_order.avsc`**:

```json
{
  "type": "record",
  "name": "SalesOrder",
  "namespace": "data_platform.sales",
  "doc": "Sales order event — v1",
  "fields": [
    {"name": "order_id",       "type": "string"},
    {"name": "customer_id",    "type": "string"},
    {"name": "order_date",     "type": "string"},
    {"name": "total_amount",   "type": "double"},
    {"name": "status",         "type": "string"},
    {"name": "chaos_metadata", "type": ["null", "string"], "default": null,
     "doc": "Chaos engineering metadata — null on clean data"}
  ]
}
```

---

### SPEC-02: Outbox Pattern

**Objective**: Guarantee exactly-once delivery between PostgreSQL and Kafka without Two-Phase Commit.

**Flow**:

```
Service.create_order(data)
  └─ BEGIN TRANSACTION
      ├─ INSERT INTO sales.sales_order VALUES (...)
      └─ INSERT INTO sales.outbox (aggregate='SalesOrder', event_type='OrderCreated', payload=..., processed=false)
  └─ COMMIT  ← atomic

OutboxRelay (Airflow DAG or Debezium CDC)
  └─ SELECT * FROM sales.outbox WHERE processed = false ORDER BY created_at LIMIT 1000
      ├─ kafka_producer.publish_batch(records)
      └─ UPDATE sales.outbox SET processed=true, processed_at=NOW() WHERE id IN (...)
```

**Migration SQL**:

```sql
-- For each transactional domain schema
CREATE TABLE sales.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate       VARCHAR(100) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    aggregate_id    VARCHAR(255) NOT NULL,
    payload         JSONB        NOT NULL,
    metadata        JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    processed       BOOLEAN      NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ,
    attempts        INTEGER      NOT NULL DEFAULT 0,
    last_error      TEXT
);

CREATE INDEX idx_outbox_unprocessed ON sales.outbox (created_at) WHERE processed = FALSE;
```

**`dbapi/api/services/outbox_relay.py`**:

```python
async def relay_outbox_batch(session: AsyncSession, producer: DataPlatformProducer, batch_size: int = 1000):
    result = await session.execute(
        select(OutboxModel)
        .where(OutboxModel.processed == False)
        .order_by(OutboxModel.created_at)
        .limit(batch_size)
        .with_for_update(skip_locked=True)  # Avoids lock contention with multiple workers
    )
    records = result.scalars().all()
    if not records:
        return 0

    published = producer.publish_batch([r.payload for r in records])
    ids = [r.id for r in records]
    await session.execute(
        update(OutboxModel)
        .where(OutboxModel.id.in_(ids))
        .values(processed=True, processed_at=func.now())
    )
    await session.commit()
    return published
```

---

### SPEC-03: OpenTelemetry Observability Stack

**Objective**: Unified traces, metrics, and structured logs.

**Stack**:

```
dbapi (OpenTelemetry SDK)
    │ OTLP gRPC
    ▼
otel-collector
    ├─► Jaeger (traces)    — http://localhost:16686
    ├─► Prometheus (metrics) — http://localhost:9090
    └─► Loki (logs)        — http://localhost:3100

Grafana ─────────────────── http://localhost:3000
  ├─ Datasource: Jaeger
  ├─ Datasource: Prometheus
  └─ Datasource: Loki
```

**`dbapi/api/config/telemetry.py`**:

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor


def configure_telemetry(app):
    provider = TracerProvider()
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint="http://otel-collector:4317"))
    )
    trace.set_tracer_provider(provider)

    FastAPIInstrumentor.instrument_app(app)
    SQLAlchemyInstrumentor().instrument(enable_commenter=True, commenter_options={})


# Custom metrics for bulk inserts
meter = metrics.get_meter("data_platform.dbapi")
bulk_insert_counter  = meter.create_counter("bulk_inserts_total", description="Total bulk inserts by domain")
bulk_insert_duration = meter.create_histogram("bulk_insert_duration_ms", description="Bulk insert duration in ms")
chaos_injection_rate = meter.create_gauge("chaos_injection_rate", description="Current chaos rate per domain")
```

---

### SPEC-04: dbt Project (Silver → Gold)

**Objective**: Replace ad-hoc Spark SQL with versioned, tested dbt models for the gold layer.

**dbt project structure**:

```
dbt/
├── dbt_project.yml
├── profiles.yml              # Points to Trino (Delta catalog) or PostgreSQL
├── models/
│   ├── staging/              # Basic cleanup — 1:1 with bronze Delta
│   │   ├── stg_sales_orders.sql
│   │   ├── stg_person_customers.sql
│   │   └── stg_warehouse_transactions.sql
│   ├── intermediate/         # Joins, deduplication, enrichment
│   │   ├── int_sales_enriched.sql
│   │   └── int_customer_360.sql
│   └── marts/                # Gold layer — final analytical models
│       ├── sales/
│       │   ├── fct_sales_orders.sql
│       │   └── dim_customers.sql
│       └── operations/
│           ├── fct_warehouse_movements.sql
│           └── fct_purchase_orders.sql
├── tests/                    # dbt data tests
│   ├── assert_no_negative_revenue.sql
│   └── assert_valid_order_statuses.sql
└── macros/
    ├── detect_chaos.sql      # Macro to detect records with chaos flags
    └── audit_columns.sql     # Macro to add audit columns
```

**`dbt/models/staging/stg_sales_orders.sql`**:

```sql
-- Detects and flags records with chaos engineering anomalies
WITH source AS (
    SELECT * FROM {{ source('iceberg_bronze', 'sales_orders') }}
),
validated AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        total_amount,
        status,
        -- Chaos detection
        CASE
            WHEN total_amount < 0                       THEN 'NEGATIVE_AMOUNT'
            WHEN customer_id IS NULL                    THEN 'NULL_FK'
            WHEN order_date > CURRENT_TIMESTAMP         THEN 'FUTURE_DATE'
            WHEN total_amount > 500000                  THEN 'OUTLIER_HIGH_VALUE'
            WHEN payment_status = 'pending'
                 AND order_date < NOW() - INTERVAL '30' DAY THEN 'GHOST_ORDER'
            ELSE NULL
        END AS data_quality_flag,
        _ingested_at,
        _chaos_metadata
    FROM source
)
SELECT * FROM validated
```

**Airflow integration**:

```python
# airflow/dags/dbt_transform_dag.py
from airflow.operators.bash import BashOperator

dbt_run = BashOperator(
    task_id="dbt_run_silver_gold",
    bash_command="cd /opt/dbt && dbt run --target prod --select marts/+",
)

dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/dbt && dbt test --target prod",
)

dbt_run >> dbt_test
```

---

### SPEC-05: Cursor-Based Pagination and Redis Cache

**Objective**: Efficient read endpoints and dimension caching.

**`dbapi/api/schemas/pagination.py`**:

```python
from typing import Generic, List, Optional, TypeVar
from pydantic import BaseModel

T = TypeVar("T")

class PageResponse(BaseModel, Generic[T]):
    data: List[T]
    next_cursor: Optional[str] = None
    has_more: bool
    total_count: Optional[int] = None  # Optional — expensive on large tables
```

**`dbapi/api/repositories/base_repository.py`** (addition to the existing pattern):

```python
async def get_page(
    session: AsyncSession,
    model: Type[Base],
    cursor: Optional[str],
    limit: int = 100,
    order_col: str = "created_at"
) -> PageResponse:
    stmt = select(model).order_by(getattr(model, order_col))
    if cursor:
        # Decode cursor (base64 encoded timestamp:id)
        cursor_val = decode_cursor(cursor)
        stmt = stmt.where(getattr(model, order_col) > cursor_val)
    stmt = stmt.limit(limit + 1)  # Fetch one extra to determine has_more
    result = await session.execute(stmt)
    rows = result.scalars().all()
    has_more = len(rows) > limit
    rows = rows[:limit]
    next_cursor = encode_cursor(rows[-1]) if has_more and rows else None
    return PageResponse(data=rows, next_cursor=next_cursor, has_more=has_more)
```

**`dbapi/api/utils/cache.py`**:

```python
import redis.asyncio as aioredis
from api.config.settings import settings

redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

def cached(prefix: str, ttl: int = 3600):
    """Cache-Aside decorator for repository methods."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            key = f"{prefix}:{':'.join(str(a) for a in args[1:])}"  # Skip 'self'
            cached_val = await redis_client.get(key)
            if cached_val:
                return cached_val
            result = await func(*args, **kwargs)
            await redis_client.setex(key, ttl, result)
            return result
        return wrapper
    return decorator

# Usage in dimension repositories
@cached("product", ttl=3600)
async def get_product_by_id(self, session, product_id: UUID) -> Product: ...

@cached("customer", ttl=1800)
async def get_customer_by_id(self, session, customer_id: UUID) -> Customer: ...
```

---

### SPEC-06: GitHub Actions CI/CD

**`/.github/workflows/ci.yml`**:

```yaml
name: CI

on:
  push:
    branches: [main, devbranch]
  pull_request:
    branches: [main]

jobs:
  lint:
    name: Lint & Type Check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
        with:
          python-version: "3.12"
      - run: uv pip install ruff mypy
      - run: ruff check dbapi/
      - run: mypy dbapi/api/ --ignore-missing-imports

  test:
    name: Unit Tests
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: admin
          POSTGRES_PASSWORD: admin
          POSTGRES_DB: admin
        ports: ["5432:5432"]
        options: --health-cmd pg_isready --health-interval 10s --health-timeout 5s --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v3
      - run: cd dbapi && uv sync
      - run: cd dbapi && uv run pytest api/tests/ -v --cov=api --cov-report=xml
      - uses: codecov/codecov-action@v4

  docker:
    name: Docker Build
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: docker build -t data-platform-dbapi:${{ github.sha }} dbapi/

  secret-scan:
    name: Secret Detection
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: trufflesecurity/trufflehog@main
        with:
          path: ./
          base: ${{ github.event.repository.default_branch }}
```

---

### SPEC-07: OpenLineage in the DAGs

**Objective**: Automatic data lineage traceability with zero extra code in the DAGs.

**`airflow/airflow.cfg`** (relevant section):

```ini
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "/api/v1/lineage"}
```

**`docker-compose` for Marquez**:

```yaml
marquez:
  image: marquezproject/marquez:latest
  ports:
    - "5000:5000"   # API
    - "5001:5001"   # Admin
  environment:
    MARQUEZ_CONFIG: /opt/marquez/marquez.config.yml
  networks:
    - hadoopcluster

marquez-web:
  image: marquezproject/marquez-web:latest
  ports:
    - "3001:3000"
  environment:
    MARQUEZ_HOST: marquez
    MARQUEZ_PORT: 5000
  networks:
    - hadoopcluster
```

With OpenLineage configured, every DAG run automatically records:
- Input datasets (PostgreSQL tables, MinIO files)
- Output datasets (Delta Lake tables, Kafka topics)
- Applied transformations (Spark jobs, dbt models)
- Quality metrics (rows written, schema changes)

---

### SPEC-08: Alembic Migrations

**Objective**: Database schema versioning with controlled rollback.

```bash
# Initial setup
cd dbapi
uv run alembic init api/migrations
```

**`dbapi/api/migrations/env.py`** (adaptation):

```python
from api.config.base import Base
from api.models.sales import *      # noqa — imports required for autogenerate
from api.models.person import *     # noqa
from api.models.warehouse import *  # noqa
# ... remaining domains

target_metadata = Base.metadata
```

**Workflow**:

```bash
# Generate a migration after a model change
uv run alembic revision --autogenerate -m "add outbox table to sales schema"

# Apply
uv run alembic upgrade head

# Rollback
uv run alembic downgrade -1
```

---

### SPEC-09: Unified Makefile

**`/Makefile`**:

```makefile
.PHONY: help start stop seed test lint build network

PYTHON := uv run python

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

network:  ## Create Docker network (run first)
	docker network create hadoopcluster 2>/dev/null || true

start: network  ## Start all platform services
	cd postgres  && docker-compose up -d
	cd minio     && docker-compose up -d
	cd nessie    && docker-compose up -d
	cd hive      && docker-compose up -d
	cd spark     && docker-compose up -d
	cd trino     && docker-compose up -d
	cd airflow   && docker-compose up -d
	cd kafka     && docker-compose up -d
	@echo "✅ All services started. Airflow: http://localhost:8181 | Kafka UI: http://localhost:8082 | Trino: http://localhost:8070"

stop:  ## Stop all platform services
	for svc in airflow kafka spark trino hive nessie minio postgres; do \
		cd $$svc && docker-compose down && cd ..; \
	done

seed:  ## Run all seeders with dynamic volumes (requires API running)
	$(PYTHON) dbapi/api/seeder/humanresources/hr_seeder.py --min-count 50  --max-count 100
	$(PYTHON) dbapi/api/seeder/product/product_seeder.py   --min-count 500 --max-count 1500
	$(PYTHON) dbapi/api/seeder/person/customer_seeder.py   --min-count 10000 --max-count 100000
	$(PYTHON) dbapi/api/seeder/sales/sales_order_seeder.py --min-count 100000 --max-count 1000000

api:  ## Start the FastAPI development server
	cd dbapi && uv run python api/app/main.py

test:  ## Run unit tests
	cd dbapi && uv run pytest api/tests/ -v --cov=api --cov-report=term-missing

lint:  ## Run linters
	uv run ruff check dbapi/
	uv run mypy dbapi/api/ --ignore-missing-imports

migrate:  ## Apply database migrations
	cd dbapi && uv run alembic upgrade head

build:  ## Build Docker images
	docker build -t data-platform-dbapi:latest dbapi/

env:  ## Copy example env file
	@[ ! -f configurations/.env ] && cp configurations/.env.example configurations/.env && echo "✅ .env created" || echo "⚠️  .env already exists"

diagram:  ## Regenerate architecture diagram
	$(PYTHON) diagram.py
```

---

### SPEC-10: CQRS — Read Path via Trino

**Objective**: Separate analytical reads from OLTP PostgreSQL, using Trino over Delta Lake for heavy queries.

**`dbapi/api/routers/analytics_router.py`**:

```python
from fastapi import APIRouter, Query, Depends
from api.services.analytics_service import AnalyticsService
from api.database.trino_connection import get_trino_conn

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/sales/summary")
async def get_sales_summary(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date:   str = Query(..., description="YYYY-MM-DD"),
    granularity: str = Query("day", regex="^(day|week|month)$"),
    conn = Depends(get_trino_conn)
):
    """
    Analytical read via Trino over Iceberg — never touches the OLTP PostgreSQL.
    Supports queries over billions of records with no impact on the transactional system.
    """
    return await AnalyticsService.sales_summary(conn, start_date, end_date, granularity)
```

**`dbapi/api/database/trino_connection.py`**:

```python
from trino.dbapi import connect
from api.config.settings import settings

def get_trino_conn():
    conn = connect(
        host=settings.TRINO_HOST,
        port=settings.TRINO_PORT,
        user=settings.TRINO_USER,
        catalog="iceberg",
        schema="sales",
    )
    try:
        yield conn
    finally:
        conn.close()
```

---

## Part 4 — README Badge Template

Add to the existing `README.md`:

```markdown
![CI](https://github.com/dougdss89/data_plataform/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6-231F20)
![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-1.5-1E4DB7)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-017CEE)
![License](https://img.shields.io/badge/license-MIT-green)
```

---

## Part 5 — Architecture Decision Records (ADRs)

Create a `docs/adr/` folder with documented architectural decisions:

| ADR | Decision | Status |
|-----|---------|--------|
| ADR-001 | Use async SQLAlchemy with bulk insert (no for-loops) | Accepted |
| ADR-002 | Delta Lake + Unity Catalog OSS for governance and versioning | Accepted |
| ADR-003 | Chaos Engineering as a data quality strategy | Accepted |
| ADR-004 | Kafka as a buffer layer between the API and storage | Proposed |
| ADR-005 | Outbox Pattern for exactly-once guarantees with Kafka | Proposed |
| ADR-006 | CQRS with Trino to separate read/write paths | Proposed |
| ADR-007 | dbt as the silver → gold transformation layer | Proposed |
| ADR-008 | OpenLineage for lineage traceability | Proposed |

---

## Executive Summary of the Debate

### What's excellent (don't change)

1. **3-layer architecture** (Repository → Service → Router) — clean, disciplined implementation
2. **Chaos Engineering** with 35+ strategies — a real portfolio differentiator
3. **Delta Lake + Unity Catalog OSS + Trino** — a lakehouse stack aligned with Databricks and market standards
4. **Universal AuditMixin** — good record-traceability practice
5. **Bulk insert with no for-loops** — demonstrates performance-at-scale knowledge

### What to add (highest impact)

1. **Kafka + Schema Registry** — turns it from "ingestion API" into "event-driven platform"
2. **Outbox Pattern** — demonstrates distributed consistency knowledge
3. **OpenTelemetry** — professional observability that recruiters recognize
4. **dbt + Great Expectations** — complete data quality cycle
5. **GitHub Actions CI** + **Makefile** — shows software engineering maturity

### What the portfolio communicates with these improvements

> *"An engineer who builds distributed data systems with consistency guarantees, automated data quality, end-to-end observability, and solid software engineering practices — ready to operate at production scale."*

---

*Document generated on: 2026-04-21*
*Reviewed by: simulated debate between Rafael (Streaming), Beatriz (Data Quality), and Lucas (Architecture)*
