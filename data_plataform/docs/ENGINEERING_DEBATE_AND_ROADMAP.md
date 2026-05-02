# Data Platform — Engineering Debate & Implementation Roadmap

> **Contexto**: Revisão arquitetural colaborativa simulando um debate entre três engenheiros sênior para identificar lacunas, sugerir melhorias e priorizar um roadmap de evolução. Destinado ao portfólio pessoal de engenharia de dados.

> **Restrição de hardware documentada**: O `dbapi` (FastAPI) **não roda como serviço online persistente** neste portfólio. Toda a carga é acionada pelo Airflow via **BashOperator** que invoca os seeders como scripts CLI (`uv run python seeder.py --min-count X --max-count Y`). A arquitetura 3-layer (Repository → Service → Router) existe no código e demonstra o padrão corretamente — mas o runtime é batch/CLI, não HTTP. As sugestões do roadmap que dependem de API online (JWT Auth, Rate Limiting, Cursor Pagination, Redis Cache-Aside para reads de usuário) são **aspiracionais** — corretas para produção, mas inaplicáveis na configuração atual por limitação de hardware.

---

## Participantes

| Papel | Nome | Especialidade |
|---|---|---|
| Senior Data Engineer | **Rafael** | Streaming, Kafka, sistemas event-driven |
| Senior Data Engineer | **Beatriz** | Data quality, dbt, platform architecture |
| Principal Data Architect | **Lucas** | Distributed systems, performance, portfolio design |

---

## Parte 1 — Debate Técnico

### Rodada 1: Ingestão e Streaming (Kafka)

---

**Rafael:** Vou direto ao ponto. A API está assumindo um papel de **ingestão síncrona** para volumes que podem chegar a 50 milhões de registros por execução. O endpoint recebe a payload, passa pelo service, e espera o bulk insert terminar antes de responder. Isso é um bottleneck de design, não de implementação.

O problema não é o `insert(Model).values(matrix)` — esse padrão está correto. O problema é que a API é ao mesmo tempo o **produtor**, o **processador** e o **escritor**. Se o Airflow chamar o seeder diretamente via BashOperator enquanto três DAGs rodam em paralelo para `sales`, `purchasing` e `warehouse` simultaneamente, temos contenção de conexão no PostgreSQL e nenhuma backpressure.

**Proposta**: Introduzir Apache Kafka como camada de buffer entre o seeder e o banco. O seeder publica eventos no tópico. Um consumer group (workers assíncronos) consome e faz o bulk insert. O Airflow passa a orquestrar produtores, não escritores diretos.

```
Seeder (Producer) → Kafka Topic → Consumer Group → PostgreSQL Bulk Insert
                                                  → Iceberg (via Spark Structured Streaming)
```

---

**Beatriz:** Concordo com o racional do Kafka, mas preciso qualificar: para o portfólio, a implementação de Kafka sem **Schema Registry** e **Data Contracts** é um anti-padrão que vai atrás de você. Vi projetos onde o producer mudou o schema do evento e o consumer quebrou silenciosamente por três dias.

O projeto já tem `apache-airflow-providers-apache-kafka==1.12.0` como dependência — o provider já está declarado, mas não há nenhuma DAG usando-o. Isso é uma lacuna explícita.

Proposta complementar ao Rafael: usar **Apache Avro + Confluent Schema Registry** (ou Karapace, que é open source) para contratos de schema entre producers e consumers. Cada domínio tem seu próprio tópico com schema versionado:

```
kafka-topics:
  - data_platform.sales.orders.v1
  - data_platform.person.customers.v1
  - data_platform.warehouse.transactions.v1
  - data_platform.dlq.poison_pills  (dead letter queue)
```

---

**Lucas:** Vou trazer a perspectiva de portfólio. Kafka com Schema Registry é correto, mas precisamos implementar o **Outbox Pattern** na API para garantir entrega exactly-once no caso de falha entre o insert no PostgreSQL e a publicação no Kafka. Sem isso, o portfólio mostra que o candidato sabe Kafka, mas não sabe os problemas que Kafka introduz.

O Outbox Pattern funciona assim:

1. O service faz o insert da entidade **e** o insert no `outbox` em uma única transação.
2. Um processo separado (Debezium CDC ou job Airflow) lê a tabela `outbox` e publica no Kafka.
3. Após confirmação do Kafka, marca o registro como `processed`.

```sql
-- Tabela outbox (por schema/domínio)
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

Isso transforma o projeto de "API que usa Kafka" para "sistema event-driven com garantias de consistência" — diferença enorme em entrevista.

---

### Rodada 2: Observabilidade e Qualidade de Dados

---

**Beatriz:** Aqui está meu maior problema com o projeto atual: o **caos engineering** está excelente — `chaos_engineering.py` com 35+ estratégias é impressionante para portfólio. Mas não há nada que **detecte** o caos que foi injetado.

O ciclo completo de engenharia de dados é:
```
Ingest → Transform → Validate → Monitor → Alert → Remediate
```

O projeto cobre bem `Ingest` e parcialmente `Transform`. Mas `Validate`, `Monitor`, `Alert` e `Remediate` estão ausentes.

**Proposta concreta**:

1. **dbt** para transformações silver → gold com `dbt tests` nativos (`not_null`, `unique`, `accepted_values`, `relationships`)
2. **Great Expectations** ou **Soda** integrado às DAGs para validação após cada carga
3. **Apache Superset** ou **Metabase** para dashboards de data quality
4. Tabela `data_quality_log` no PostgreSQL para histórico de anomalias detectadas

O grande showcase é: "Injetei dados sujos intencionalmente no bronze e construí um pipeline que os detecta, quarentena e reporta automaticamente no silver."

---

**Rafael:** Validação de dados é necessária, mas observabilidade da plataforma é igualmente crítica. Hoje não há nada que responda: "Quanto tempo levou o bulk insert de 5M de registros? Qual foi o throughput? Houve degradação ao longo do tempo?"

**Proposta**: OpenTelemetry como camada unificada de observabilidade:

```python
# dbapi/api/config/telemetry.py
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

tracer = trace.get_tracer("data_platform.dbapi")

# Uso nos services
async def bulk_insert_orders(session, data):
    with tracer.start_as_current_span("sales.orders.bulk_insert") as span:
        span.set_attribute("record_count", len(data))
        span.set_attribute("chaos_rate", chaos_rate)
        # ... insert logic
        span.set_attribute("duration_ms", elapsed)
```

Stack de observabilidade para o portfólio:
- **Traces**: OpenTelemetry → Jaeger ou Tempo
- **Metrics**: Prometheus + Grafana (scrape da API e dos workers Kafka)
- **Logs**: Structured JSON → Loki ou OpenSearch
- **Alertas**: AlertManager para SLA de DAGs

---

**Lucas:** Ótimo, mas quero conectar os dois pontos. O projeto tem `apache-airflow-providers-openlineage==1.9` instalado e **não está sendo usado**. OpenLineage é o padrão de data lineage que a Marquez/OpenMetadata consome.

Se o projeto instrumentar OpenLineage nas DAGs, o portfólio demonstra rastreabilidade de linhagem de dados de ponta a ponta:

```
PostgreSQL (source) → Spark Job (transform) → Delta Lake (sink)
                   OpenLineage event →  Marquez UI (lineage graph)
```

Isso é nível de empresa de dados de ponta. Candidatos que sabem lineage automaticamente passam pelo filtro de engenheiros plenos e são avaliados como sênior.

---

### Rodada 3: Performance e Padrões de Projeto

---

**Lucas:** Agora vou ser direto sobre os padrões de projeto. A arquitetura 3-layer (Repository → Service → Router) está correta, mas está implementada como **CRUD puro**. Para um portfólio de engenharia de dados, precisamos mostrar padrões mais sofisticados.

**CQRS (Command Query Responsibility Segregation)** é o mais relevante aqui:

```
Write Path (Commands): FastAPI → Service → Repository → PostgreSQL → Kafka → Delta Lake
Read  Path (Queries):  FastAPI → Query Service → Trino (federated SQL over Delta Lake)
```

Por que isso importa: hoje se alguém quiser consultar 50M de registros de `sales`, a query vai direto no PostgreSQL OLTP. Com CQRS, leituras analíticas vão para Trino sobre Delta Lake (OLAP) e escritas vão para PostgreSQL (OLTP). O projeto já tem Trino, mas não há nenhum endpoint da API que o usa.

---

**Rafael:** Concordo com CQRS, mas quero adicionar **Event Sourcing parcial** para os domínios transacionais. Em vez de armazenar apenas o estado atual de um pedido, armazenamos a sequência de eventos:

```
OrderCreated → OrderPaymentConfirmed → OrderShipped → OrderDelivered
```

Cada evento é imutável. O estado atual é a projeção (fold) dos eventos. Isso é natural com Kafka — o log do Kafka **é** o event store.

Benefício para portfólio: demonstra understanding de consistência eventual, projeções, e replay de eventos — conceitos que aparecem em entrevistas para posições sênior em empresas como iFood, Nubank, Mercado Livre.

---

**Beatriz:** Vou trazer um ponto mais pragmático sobre performance que ninguém mencionou: **não há paginação nem cursor-based pagination nos routers**.

```python
# Como está hoje (presumido)
@router.get("/orders")
async def list_orders(db: AsyncSession = Depends(get_db)):
    return await order_service.get_all(db)  # SELECT * — sem limite

# Como deveria ser
@router.get("/orders")
async def list_orders(
    cursor: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db)
):
    return await order_service.get_page(db, cursor=cursor, limit=limit)
```

Cursor-based pagination é mais eficiente que offset pagination para tabelas grandes (no offset, o banco lê e descarta os primeiros N registros). Com 50M de registros em `sales`, isso é crítico.

Segundo ponto: **Redis não está sendo usado para cache**, apesar de estar configurado no `.env`. Padrão Cache-Aside para queries frequentes de dimensões (produtos, clientes, localizações):

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

### Rodada 4: Segurança e CI/CD

---

**Beatriz:** Segurança é o ponto mais crítico para portfólio. As credenciais `minioadmin:minioadmin` e `admin:admin` no `.env` são aceitáveis para desenvolvimento local, mas o portfólio precisa mostrar que o candidato sabe o que **não fazer em produção**.

**O que adicionar**:
1. **Makefile** ou script de setup que gera `.env` a partir de `.env.example` (nunca commitar o `.env` real)
2. **GitHub Actions** com `trufflehog` ou `detect-secrets` para bloquear commits com credenciais
3. **Docker Secrets** ou **HashiCorp Vault** (modo dev) para secrets em runtime
4. **JWT Authentication** nos routers da API com FastAPI Security

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

**Rafael:** CI/CD é ausente e é uma das primeiras coisas que recrutadores olham no GitHub. O projeto precisa de pelo menos:

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

Além disso, **pre-commit hooks** com `ruff`, `mypy`, `detect-secrets` e `hadolint` (linter de Dockerfile).

---

**Lucas:** Para fechar o debate, quero falar sobre o que diferencia este portfólio de outros 95% de projetos no GitHub:

1. **README com arquitetura visual** — o `diagram.py` existe mas a imagem não aparece no README
2. **Demo video / GIF** — mostrar o Airflow rodando, o Trino consultando, o chaos engineering em ação
3. **Makefile unificado** — `make start`, `make seed`, `make test`, `make stop` para facilitar reprodução
4. **ARCHITECTURE.md** separado com decisões arquiteturais (ADRs — Architecture Decision Records)
5. **Badges no README** — CI passing, coverage %, Python version, license

O que faz um portfólio de engenharia de dados se destacar não é só a complexidade técnica — é a **comunicação da complexidade técnica**. Um recrutador técnico passa 3 minutos no README antes de decidir continuar.

---

## Parte 2 — Roadmap de Implementação

### Priorização por Impacto × Esforço

> **Legenda de modo**: `CLI` = aplica-se ao modo atual (BashOperator); `ONLINE` = requer API rodando como serviço (aspiracional); `INFRA` = componente de infraestrutura independente do modo da API.

| # | Item | Modo | Impacto Portfólio | Esforço | Prioridade |
|---|------|:----:|:-----------------:|:-------:|:----------:|
| 1 | README com arquitetura e badges | INFRA | Alto | Baixo | **P0** |
| 2 | Makefile unificado (`make start/stop/seed/test`) | INFRA | Alto | Baixo | **P0** |
| 3 | `.env.example` + remoção de credenciais do git | INFRA | Alto | Baixo | **P0** |
| 4 | GitHub Actions CI (lint + test + build) | INFRA | Alto | Médio | **P0** |
| 5 | Kafka (producer no seeder CLI) + consumer group | CLI | Alto | Alto | **P1** |
| 6 | Schema Registry (Karapace) + Avro schemas | CLI | Alto | Alto | **P1** |
| 7 | dbt project (silver → gold transforms) | INFRA | Alto | Alto | **P1** |
| 8 | OpenLineage nas DAGs + Marquez UI | INFRA | Alto | Médio | **P1** |
| 9 | Alembic migrations (hoje ausente) | CLI | Médio | Médio | **P1** |
| 10 | pytest integração com fixtures AsyncSession | CLI | Médio | Médio | **P1** |
| 11 | Outbox Pattern (tabela + Debezium CDC) | CLI | Alto | Alto | **P2** |
| 12 | Great Expectations integrado nas DAGs | INFRA | Médio | Médio | **P2** |
| 13 | Prometheus + Grafana stack | INFRA | Médio | Médio | **P2** |
| 14 | OpenTelemetry nos seeders CLI (sem HTTP) | CLI | Médio | Médio | **P2** |
| 15 | CQRS Read Path via Trino (DAG de query) | INFRA | Médio | Alto | **P2** |
| 16 | Cursor-based pagination nos routers | ONLINE | Baixo | Médio | **P3** |
| 17 | Redis Cache-Aside para dimensões | ONLINE | Baixo | Médio | **P3** |
| 18 | JWT Authentication nos routers | ONLINE | Baixo | Médio | **P3** |
| 19 | Event Sourcing para `sales` domain | CLI | Médio | Alto | **P3** |
| 20 | HashiCorp Vault (modo dev) | INFRA | Baixo | Alto | **P3** |

---

## Parte 3 — Especificações de Implementação

### SPEC-01: Apache Kafka Integration

**Objetivo**: Desacoplar produção e consumo de dados, adicionar backpressure e suporte a Structured Streaming via Spark.

**Arquitetura**:

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

**Arquivos a criar**:

```
kafka/
├── docker-compose.yml          # Kafka + Zookeeper + Karapace (Schema Registry)
├── config/
│   ├── server.properties       # Kafka broker config
│   └── karapace.config.json    # Schema registry config
dbapi/api/
├── kafka/
│   ├── producer.py             # KafkaProducer com Avro serialization
│   ├── consumer.py             # KafkaConsumer base class com bulk insert
│   └── schemas/                # Avro schemas por domínio
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
    Kafka producer com Avro serialization e Schema Registry.
    Garante que todos os eventos publicados estejam em conformidade com o schema registrado.
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
     "doc": "Chaos engineering metadata — null em dados limpos"}
  ]
}
```

---

### SPEC-02: Outbox Pattern

**Objetivo**: Garantir entrega exactly-once entre PostgreSQL e Kafka sem Two-Phase Commit.

**Fluxo**:

```
Service.create_order(data)
  └─ BEGIN TRANSACTION
      ├─ INSERT INTO sales.sales_order VALUES (...)
      └─ INSERT INTO sales.outbox (aggregate='SalesOrder', event_type='OrderCreated', payload=..., processed=false)
  └─ COMMIT  ← atomic

OutboxRelay (Airflow DAG ou Debezium CDC)
  └─ SELECT * FROM sales.outbox WHERE processed = false ORDER BY created_at LIMIT 1000
      ├─ kafka_producer.publish_batch(records)
      └─ UPDATE sales.outbox SET processed=true, processed_at=NOW() WHERE id IN (...)
```

**Migration SQL**:

```sql
-- Para cada schema de domínio transacional
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
        .with_for_update(skip_locked=True)  # Evita lock contention com múltiplos workers
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

**Objetivo**: Traces, métricas e logs estruturados unificados.

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


# Métricas customizadas para bulk inserts
meter = metrics.get_meter("data_platform.dbapi")
bulk_insert_counter  = meter.create_counter("bulk_inserts_total", description="Total bulk inserts by domain")
bulk_insert_duration = meter.create_histogram("bulk_insert_duration_ms", description="Bulk insert duration in ms")
chaos_injection_rate = meter.create_gauge("chaos_injection_rate", description="Current chaos rate per domain")
```

---

### SPEC-04: dbt Project (Silver → Gold)

**Objetivo**: Substituir Spark SQL ad-hoc por modelos dbt versionados e testados para camada gold.

**Estrutura do projeto dbt**:

```
dbt/
├── dbt_project.yml
├── profiles.yml              # Aponta para Trino (catálogo Delta) ou PostgreSQL
├── models/
│   ├── staging/              # Limpeza básica — 1:1 com bronze Delta
│   │   ├── stg_sales_orders.sql
│   │   ├── stg_person_customers.sql
│   │   └── stg_warehouse_transactions.sql
│   ├── intermediate/         # Joins, deduplicação, enriquecimento
│   │   ├── int_sales_enriched.sql
│   │   └── int_customer_360.sql
│   └── marts/                # Camada gold — modelos analíticos finais
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
    ├── detect_chaos.sql      # Macro para detectar registros com flags de chaos
    └── audit_columns.sql     # Macro para adicionar colunas de auditoria
```

**`dbt/models/staging/stg_sales_orders.sql`**:

```sql
-- Detecta e marca registros com anomalias do chaos engineering
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
        -- Detecção de chaos
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

**Integração com Airflow**:

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

### SPEC-05: Cursor-Based Pagination e Redis Cache

**Objetivo**: Endpoints de leitura eficientes e caching de dimensões.

**`dbapi/api/schemas/pagination.py`**:

```python
from typing import Generic, List, Optional, TypeVar
from pydantic import BaseModel

T = TypeVar("T")

class PageResponse(BaseModel, Generic[T]):
    data: List[T]
    next_cursor: Optional[str] = None
    has_more: bool
    total_count: Optional[int] = None  # Opcional — caro em tabelas grandes
```

**`dbapi/api/repositories/base_repository.py`** (adição ao padrão existente):

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
    """Cache-Aside decorator para métodos de repositório."""
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

# Uso nos repositories de dimensões
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

### SPEC-07: OpenLineage nas DAGs

**Objetivo**: Rastreabilidade de linhagem de dados automática com zero código adicional nas DAGs.

**`airflow/airflow.cfg`** (seção relevante):

```ini
[openlineage]
transport = {"type": "http", "url": "http://marquez:5000", "endpoint": "/api/v1/lineage"}
```

**`docker-compose` para Marquez**:

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

Com OpenLineage configurado, cada execução de DAG automaticamente registra:
- Datasets de entrada (tabelas PostgreSQL, arquivos MinIO)
- Datasets de saída (tabelas Delta Lake, tópicos Kafka)
- Transformações aplicadas (Spark jobs, dbt models)
- Métricas de qualidade (rows written, schema changes)

---

### SPEC-08: Alembic Migrations

**Objetivo**: Versionamento de schema do banco de dados com rollback controlado.

```bash
# Setup inicial
cd dbapi
uv run alembic init api/migrations
```

**`dbapi/api/migrations/env.py`** (adaptação):

```python
from api.config.base import Base
from api.models.sales import *      # noqa — imports necessários para autogenerate
from api.models.person import *     # noqa
from api.models.warehouse import *  # noqa
# ... demais domínios

target_metadata = Base.metadata
```

**Workflow**:

```bash
# Gerar migration após mudança de model
uv run alembic revision --autogenerate -m "add outbox table to sales schema"

# Aplicar
uv run alembic upgrade head

# Rollback
uv run alembic downgrade -1
```

---

### SPEC-09: Makefile Unificado

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

**Objetivo**: Separar reads analíticos do OLTP PostgreSQL, usando Trino sobre Delta Lake para queries pesadas.

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
    Leitura analítica via Trino sobre Iceberg — não toca o PostgreSQL OLTP.
    Suporta queries sobre bilhões de registros sem impacto no sistema transacional.
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

## Parte 4 — README Badge Template

Adicionar ao `README.md` existente:

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

## Parte 5 — Architecture Decision Records (ADRs)

Criar pasta `docs/adr/` com decisões arquiteturais documentadas:

| ADR | Decisão | Status |
|-----|---------|--------|
| ADR-001 | Usar SQLAlchemy async com bulk insert (sem for-loops) | Aceito |
| ADR-002 | Delta Lake + Unity Catalog OSS para governança e versionamento | Aceito |
| ADR-003 | Chaos Engineering como estratégia de qualidade de dados | Aceito |
| ADR-004 | Kafka como camada de buffer entre API e armazenamento | Proposto |
| ADR-005 | Outbox Pattern para garantia exactly-once com Kafka | Proposto |
| ADR-006 | CQRS com Trino para separar read/write paths | Proposto |
| ADR-007 | dbt como camada de transformação silver → gold | Proposto |
| ADR-008 | OpenLineage para rastreabilidade de linhagem | Proposto |

---

## Resumo Executivo do Debate

### O que está excelente (não mudar)

1. **3-layer architecture** (Repository → Service → Router) — implementação limpa e disciplinada
2. **Chaos Engineering** com 35+ estratégias — diferencial real de portfólio
3. **Delta Lake + Unity Catalog OSS + Trino** — stack de lakehouse alinhado com Databricks e padrão de mercado
4. **AuditMixin** universal — boa prática de rastreabilidade de registros
5. **Bulk insert sem for-loops** — demonstra conhecimento de performance em escala

### O que adicionar (maior impacto)

1. **Kafka + Schema Registry** — transforma de "API de ingestão" para "plataforma event-driven"
2. **Outbox Pattern** — demonstra conhecimento de consistência distribuída
3. **OpenTelemetry** — observabilidade profissional que recrutadores reconhecem
4. **dbt + Great Expectations** — ciclo completo de qualidade de dados
5. **GitHub Actions CI** + **Makefile** — mostra maturidade de engenharia de software

### O que o portfólio comunica com essas melhorias

> *"Engenheiro que constrói sistemas de dados distribuídos com garantias de consistência, qualidade de dados automatizada, observabilidade de ponta a ponta e boas práticas de engenharia de software — pronto para operar em escala de produção."*

---

*Documento gerado em: 2026-04-21*
*Revisado por: debate simulado entre Rafael (Streaming), Beatriz (Data Quality) e Lucas (Architecture)*
