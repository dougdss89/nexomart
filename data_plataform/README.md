# Data Platform — NexoMart

Plataforma de dados open-source construída do zero para o case **NexoMart** — um marketplace brasileiro multi-canal que processa entre 100 mil e 50 milhões de pedidos por ciclo. O objetivo é separar a camada transacional (OLTP) da analítica (OLAP), sem depender de nenhuma cloud proprietária.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────┐
│                      INGESTÃO & API                          │
│    FastAPI (async) + SQLAlchemy 2.0 + asyncpg + Pydantic     │
├──────────────────────────────────────────────────────────────┤
│                       BANCO OLTP                             │
│     PostgreSQL 16 + pgvector + JSONB + Particionamento       │
├──────────────────────────────────────────────────────────────┤
│                      ORQUESTRAÇÃO                            │
│           Apache Airflow 2.9 (CeleryExecutor + Redis)        │
├──────────────────────────────────────────────────────────────┤
│              PROCESSAMENTO DISTRIBUÍDO (Spark 3.5.8)         │
│  Landing → Bronze → Silver (Delta Lake) → Gold (Iceberg)     │
├──────────────────────────────────────────────────────────────┤
│                  ARMAZENAMENTO ANALÍTICO                      │
│       Delta Lake + Apache Iceberg + MinIO (S3-compatible)    │
├──────────────────────────────────────────────────────────────┤
│                  CATÁLOGO & METASTORE                         │
│    Hive Metastore 4.0 (PostgreSQL) + Unity Catalog OSS       │
├──────────────────────────────────────────────────────────────┤
│                   FEDERAÇÃO DE CONSULTAS                      │
│    Apache Trino 482 (delta_lake + iceberg + nexo catalogs)   │
├──────────────────────────────────────────────────────────────┤
│                  TRANSFORMAÇÕES ANALÍTICAS                    │
│                  dbt-trino 1.10 / dbt-core 1.11              │
└──────────────────────────────────────────────────────────────┘
```

### Pipeline de Dados (Medallion Architecture)

```
PostgreSQL (OLTP)
      │
      ▼
 Landing  (Parquet bruto,  s3a://landing/)
      │  spark/jobs/landing_layer_scripts/
      ▼
 Bronze   (Delta Lake,     s3a://bronze/)
      │  spark/jobs/raw_layer_extract_scripts/
      ▼
 Silver   (Delta Lake + surrogate keys, s3a://silver/)
      │  spark/jobs/silver_layer_advworks_scripts/
      ▼
 Gold     (Apache Iceberg, s3a://deltadw/iceberglake/)
      │  spark/jobs/gold_layer_scripts/
      │  catálogo: iceberglake (HiveCatalog → HMS)
      ▼
 Trino ──► dbt (views/marts analíticos no catálogo iceberglake.gold)
```

**Formatos por camada:**

| Camada | Formato | Catálogo Spark | Catálogo Trino |
|--------|---------|---------------|----------------|
| Landing | Parquet bruto | — | `minio` |
| Bronze | Delta Lake | `spark_catalog` | `delta` |
| Silver | Delta Lake | `spark_catalog` | `delta` |
| Gold | Apache Iceberg | `iceberglake` | `iceberglake` |
| Marts dbt | Views Trino | — | `iceberglake` |

---

## Catálogos

### Spark — 3 catálogos simultâneos

| Catálogo | Implementação | Uso |
|----------|--------------|-----|
| `spark_catalog` | DeltaCatalog (padrão) | Landing, Bronze, Silver em Delta Lake |
| `iceberglake` | IcebergHiveCatalog (HMS) | Escrita Gold em Iceberg |
| `nexo` | IcebergRESTCatalog (UC OSS) | Leitura via Unity Catalog |

```python
# Gold — escrita Iceberg via HiveCatalog
df.writeTo("iceberglake.gold.gold_dim_productcategory").createOrReplace()

# Leitura via Unity Catalog REST
spark.sql("SELECT * FROM nexo.gold.gold_dim_productcategory")
```

### Trino — 4 catálogos

| Catálogo | Connector | Uso |
|----------|-----------|-----|
| `delta` | `delta_lake` + HMS | Bronze e Silver |
| `iceberglake` | `iceberg` + HMS | Gold (leitura e dbt) |
| `nexo` | `iceberg` + UC REST | Unity Catalog OSS |
| `minio` | `delta_lake` + HMS | Landing / exploração bruta |

```sql
-- Consultar Gold via Trino
SELECT * FROM iceberglake.gold.gold_dim_productcategory LIMIT 10;
SHOW TABLES IN iceberglake.gold;
```

### Unity Catalog OSS — via REST API

A GUI do UC OSS v0.4.0 requer OAuth (Google/OIDC) e não funciona sem um provider configurado. Use a REST API ou o catálogo `nexo` no Spark/Trino:

```bash
# Catálogos
curl -s http://localhost:8182/api/2.1/unity-catalog/catalogs | python3 -m json.tool

# Schemas
curl -s "http://localhost:8182/api/2.1/unity-catalog/schemas?catalog_name=unity" | python3 -m json.tool

# Tabelas
curl -s "http://localhost:8182/api/2.1/unity-catalog/tables?catalog_name=unity&schema_name=gold" | python3 -m json.tool
```

---

## Domínios de Negócio

| Schema | Domínio | Descrição |
|--------|---------|-----------|
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
├── airflow/
│   ├── dags/               # DAGs do Airflow
│   └── dockerfile          # Astro Runtime 11.4 com Spark 3.5.8 embutido
├── spark/
│   ├── jobs/               # Scripts PySpark por camada
│   │   ├── landing_layer_scripts/
│   │   ├── raw_layer_extract_scripts/    # bronze
│   │   ├── silver_layer_advworks_scripts/
│   │   └── gold_layer_scripts/           # Iceberg via iceberglake
│   └── conf/
│       └── spark-defaults.conf.template  # gerado via envsubst
├── hive/                   # Hive Metastore 4.0 (PostgreSQL backend)
├── trino/
│   └── catalog/            # delta.properties, iceberglake.properties, nexo.properties, minio.properties
├── unitycatalog/           # Unity Catalog OSS (REST API + Iceberg REST catalog)
├── configurations/
│   ├── .env                # Paths de jobs, buckets MinIO, conexões
│   ├── secrets.env         # Credenciais (não commitado)
│   ├── profiles.yml        # dbt target: iceberglake.gold via Trino
│   ├── dbt/                # Projeto dbt-trino (modelos gold)
│   └── configconnections_loader.py
└── Makefile                # Comandos de operação da stack
```

---

## Pré-requisitos

- Docker Desktop (WSL2 no Windows)
- Docker Compose v2
- Make
- Python 3.12+ com [uv](https://github.com/astral-sh/uv)

---

## Início Rápido

### Primeira vez

```bash
# 1. Configure as credenciais (nunca commitadas)
cp configurations/secrets.env.example configurations/secrets.env
# edite secrets.env com os valores reais

# 2. Cria rede, volumes e builda as imagens
make setup

# 3. Sobe toda a stack (gera configs e inicia serviços em ordem)
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
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | conforme `secrets.env` |
| Spark Master UI | http://localhost:9090 | — |
| Spark History Server | http://localhost:18080 | — |
| DataFlint (live, durante job) | http://localhost:4040/dataflint | — |
| Trino UI | http://localhost:8070 | — |
| Airflow UI | http://localhost:8181 | conforme `secrets.env` |
| Unity Catalog REST API | http://localhost:8182/api/2.1/unity-catalog | — |
| dbapi (FastAPI) | http://localhost:8001 | — |
| dbapi Docs | http://localhost:8001/docs | — |

---

## DAG de Integração End-to-End

`integration_e2e_test` — sequência completa do pipeline:

```
start → seed_postgres → pg_landing → pg_bronze → pg_silver
      → pg_gold → trino_validate → dbt_run → dbt_test → end
```

`pg_gold` usa `SparkSubmitOperator` em **client mode** (driver no `airflow_worker`). O job escreve em `iceberglake.gold.gold_dim_productcategory` via Iceberg + HiveCatalog.

```bash
# Disparar via CLI
docker exec airflow_scheduler airflow dags trigger integration_e2e_test

# Monitorar
docker exec airflow_scheduler airflow tasks states-for-dag-run \
  integration_e2e_test <run_id>
```

---

## Monitoramento de Queries Spark

### DataFlint — planos de execução enriquecidos

O DataFlint (JAR baked nas imagens Spark) adiciona análise visual de query plans.

**Live view** (durante um job em execução):
```
http://localhost:4040/dataflint
```
A porta 4040 fica ativa enquanto o driver do job estiver rodando no `airflow_worker`.

**History view** (jobs concluídos):
```
http://localhost:18080  → clique em uma app → aba "DataFlint"
                        → aba "SQL / DataFrame" (view padrão do Spark)
```

**PySpark interativo** (driver permanente para exploração):
```bash
docker exec -it airflow_worker /opt/spark/bin/pyspark \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=172.18.0.12 \
  --conf spark.driver.bindAddress=0.0.0.0
```
Enquanto o shell estiver aberto, `localhost:4040/dataflint` permanece acessível.

---

## Configurações Críticas da Stack

### spark-defaults.conf

Gerado pelo `make up` via `envsubst` a partir do template. **Nunca edite o `.conf` diretamente** — edite o `.template` e regenere:

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

Chaves obrigatórias:

```properties
# Metastore externo (sem isso Spark usa Derby local)
spark.sql.catalogImplementation      hive
spark.sql.warehouse.dir              s3a://deltadw/warehouse/
spark.hadoop.hive.metastore.uris     thrift://hive-metastore:9083

# Catálogo Iceberg via HMS (escrita Gold)
spark.sql.catalog.iceberglake                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberglake.catalog-impl      org.apache.iceberg.hive.HiveCatalog
spark.sql.catalog.iceberglake.uri               thrift://hive-metastore:9083
spark.sql.catalog.iceberglake.hive.lock-enabled false

# Catálogo Iceberg via Unity Catalog REST (leitura nexo)
spark.sql.catalog.nexo                   org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.nexo.catalog-impl      org.apache.iceberg.rest.RESTCatalog
spark.sql.catalog.nexo.uri               http://unitycatalog:8080/api/2.1/unity-catalog/iceberg

# Credenciais MinIO
spark.hadoop.fs.s3a.access.key       minioadmin
spark.hadoop.fs.s3a.secret.key       minioadmin
spark.hadoop.fs.s3a.endpoint         http://minio:9000
```

### dbt — Modelos Gold

Target: `iceberglake.gold` via Trino (`configurations/profiles.yml`).

```yaml
data_platform:
  target: dev
  outputs:
    dev:
      type: trino
      database: iceberglake
      schema: gold
```

Use `materialized='view'` para marts que são projeções diretas — sem escrita no S3, sem resíduos `__dbt_tmp` / `__dbt_backup`.

Sources declaradas em `configurations/dbt/models/gold/sources.yml`:
- `gold` → `iceberglake.gold` (tabelas Iceberg escritas pelo Spark)
- `gold_iceberg` → alias para domínios futuros (buyer)

---

## Problemas Conhecidos e Correções

### 1. Iceberg HiveCatalog: `LockException` no HMS

**Sintoma:** `LockException: Failed to find lock for table gold.gold_dim_productcategory`, precedido por `Internal error processing show_locks`.

**Causa:** O `MetastoreLock` do Iceberg chama `show_locks` via Thrift, que exige as tabelas ACID no PostgreSQL do metastore (`TXNS`, `HIVE_LOCKS`, `TXN_COMPONENTS`, `NEXT_LOCK_ID`, etc.). Em uma instalação standalone do HMS sem inicialização ACID, essas tabelas não existem.

**Correção aplicada:**
1. `spark.sql.catalog.iceberglake.hive.lock-enabled = false` no `spark-defaults.conf` (o bind mount no `airflow_worker` precisa ser forçado — veja item 4 abaixo).
2. Tabelas ACID criadas no PostgreSQL do metastore a partir do schema em `/opt/hive/scripts/metastore/upgrade/postgres/hive-schema-4.0.0-alpha-1.postgres.sql` (linhas 1710–2100).

---

### 2. Trino: `HIVE_CANNOT_OPEN_SPLIT` após recriar tabela Gold (Delta)

**Sintoma:** `SELECT *` na tabela Gold Delta falha com `Error opening Hive split`. `SELECT count(*)` passa.

**Causa:** o Trino cacheia o `_delta_log/00000000000000000000.json` em memória. Se a tabela é apagada e recriada na mesma path, o Delta Log reinicia no version 0 (mesmo nome de arquivo) mas o Trino serve o conteúdo antigo.

**Nota:** A camada Gold migrou para Iceberg (`iceberglake`), eliminando este problema para tabelas Gold. Ainda relevante para Bronze/Silver.

**Correção emergencial:**
```bash
docker restart trino-coordinator trino-trino-worker-1

# Ou via procedure (sem restart):
docker exec trino-coordinator trino \
  --execute "CALL delta.system.flush_metadata_cache(schema_name => 'gold', table_name => '<tabela>')" \
  --server http://localhost:8080 --user admin
```

---

### 3. Hive Metastore 4.0: Thrift timeout em `createTable`

**Sintoma:** `RetryingMetaStoreClient: MetaStoreClient lost connection. createTable` com `TTransportException: Read timed out`. Jobs da camada Gold falham após ~1 min.

**Causa:** transações PostgreSQL em estado `idle in transaction` bloqueiam DDL no metastore.

**Correção aplicada:**
1. `hive/conf/hive-site.xml` — pool de threads ampliado:
```xml
<property><name>hive.metastore.server.max.threads</name><value>200</value></property>
<property><name>hive.metastore.client.socket.timeout</name><value>120</value></property>
```
2. PostgreSQL — auto-kill de sessões travadas:
```sql
ALTER DATABASE metastore SET idle_in_transaction_session_timeout = '2min';
```

**Diagnóstico:**
```bash
docker exec pgprojeto psql -U admin -d metastore -c "
SELECT pid, state, now() - query_start AS duracao, query
FROM pg_stat_activity
WHERE datname = 'metastore' AND state = 'idle in transaction'
ORDER BY duracao DESC;"
```

---

### 4. WSL2: bind mount stale após `docker restart`

**Sintoma:** `docker restart <container>` falha com `OCI runtime create failed: unable to start container process: error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/..."`.

**Causa:** Docker Desktop no WSL2 usa hashes para bind mounts; um `restart` tenta reusar o hash antigo.

**Correção:**
```bash
# Nunca use docker restart para containers com bind mounts WSL2
docker rm -f <container>
docker compose --env-file configurations/secrets.env -f <service>/docker-compose.yml up -d <service>

# Exemplo para o airflow_worker (crítico: carrega spark-defaults.conf via bind mount)
docker rm -f airflow_worker
docker compose --env-file configurations/secrets.env -f airflow/docker-compose.yaml up -d --force-recreate airflow-worker
```

---

### 5. dbt: `Model depends on source which was not found`

**Sintoma:** `dbt run` falha com `Model 'gold_mart_buyer' depends on source 'gold_iceberg.gold_dim_buyer' which was not found`.

**Causa:** dbt compila todos os models antes de executar — qualquer source não declarado em `sources.yml` quebra mesmo com `--select` apontando para outro model.

**Correção aplicada:** `gold_iceberg` declarado como source em `configurations/dbt/models/gold/sources.yml`.

---

### 6. dbt: resíduos `__dbt_tmp` e `__dbt_backup` no MinIO

**Causa:** `materialized='table'` com dbt-trino usa rename atômico; falhas intermediárias deixam paths órfãos no S3.

**Limpeza:**
```bash
docker exec trino-coordinator trino --server http://localhost:8080 --user admin \
  --execute "DROP TABLE IF EXISTS iceberglake.gold.gold_mart_productcategory__dbt_backup"
```

**Prevenção:** usar `materialized='view'` para marts que são projeções diretas.

---

### 7. Airflow worker com `SQL_ALCHEMY_CONN` vazio

**Sintoma:** `airflow_worker` crasha com `FATAL: role "astro" does not exist`. Variáveis `${POSTGRES_USER}` não expandidas.

**Correção:** sempre usar o Makefile:
```bash
make up-airflow   # usa: docker compose --env-file configurations/secrets.env
```

---

## Pontos de Atenção Operacional

### `make clean` — o que é destruído e o que sobrevive

| Recurso | `make down` | `make clean` |
|---------|-------------|--------------|
| Containers | ✓ removidos | ✓ removidos |
| Volume `dockervol` | preservado | **destruído** |
| Volume `spark-logs` | preservado | **destruído** |
| Volume `spark-ivy-cache` | preservado | **destruído** |
| Dados MinIO (Delta + Iceberg) | preservado | preservado |
| Dados PostgreSQL | preservado | preservado |
| Rede `hadoopcluster` | preservado | **destruída** |

### Ordem de dependência dos serviços

```
postgres → minio → hive-metastore → unitycatalog → spark → trino → airflow → dbapi
```

### Verificar saúde do Hive Metastore

```bash
# Sessões PostgreSQL do metastore
docker exec pgprojeto psql -U admin -d metastore -c \
  "SELECT state, count(*) FROM pg_stat_activity WHERE datname='metastore' GROUP BY state;"

# Tabelas registradas no Hive
docker exec -i pgprojeto psql -U admin -d metastore -c \
  'SELECT "TBL_NAME", "TBL_TYPE" FROM "TBLS" ORDER BY "TBL_NAME";'
```

### Verificar tabelas no Trino

```bash
# Gold (Iceberg)
docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN iceberglake.gold" \
  --server http://localhost:8080 --user admin

docker exec trino-coordinator trino \
  --execute "SELECT count(*) FROM iceberglake.gold.gold_dim_productcategory" \
  --server http://localhost:8080 --user admin

# Bronze / Silver (Delta)
docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN delta.silver" \
  --server http://localhost:8080 --user admin
```

### Regenerar spark-defaults.conf

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

O `make up` chama `generate-confs` automaticamente.

---

## API de Ingestão (dbapi)

Arquitetura 3 camadas obrigatória em todos os domínios:

```
Router (FastAPI) → Service (regras de negócio) → Repository (bulk insert SQLAlchemy)
```

```bash
# Executar localmente
cd dbapi && uv run python api/app/main.py

# Rodar um seeder
uv run python dbapi/api/seeder/<dominio>/<arquivo>.py \
  --min-count 100000 --max-count 500000 \
  --min-chaos 0.01   --max-chaos 0.10

# Rodar os testes
cd dbapi && uv run python api/tests/test_product_services.py
```

---

## Chaos Engineering

Os seeders injetam entre **1% e 10%** de anomalias controladas por execução:

| Tipo | Exemplos |
|------|---------|
| Duplicidade | Mesmo registro com variações de formatação |
| Dados sujos | CPF inválido, email sem TLD, telefone `0000000` |
| Fraudes | Compras geograficamente impossíveis, CPF falso com endereço real |
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
|---------|--------|-------|
| Vendas | `SO-XXXXXXXXX` | `SO-XXXXXXXXX-XX` |
| Compras | `PO-XXXXXXXXX` | `PO-XXXXXXXXX-XX` |
| Estoque | `TR-XXXXXXXXX` | `TR-XXXXXXXXX-XX` |
| Devoluções | `RO-XXXXXXXXX` | — |

---

## Stack Completa

| Componente | Tecnologia | Versão |
|------------|-----------|--------|
| API de Ingestão | FastAPI + asyncpg | — |
| ORM | SQLAlchemy | 2.0 |
| Validação | Pydantic | 2.x |
| Banco OLTP | PostgreSQL | 16 |
| Busca Semântica | pgvector | — |
| Object Storage | MinIO | latest |
| Orquestração | Apache Airflow (Astro Runtime) | 2.9 |
| Processamento | Apache Spark | 3.5.8 |
| Formato Landing/Bronze/Silver | Delta Lake | 3.2.1 |
| Formato Gold | Apache Iceberg | 1.7.1 |
| Metastore | Hive Metastore | 4.0.0 |
| Federação SQL | Apache Trino | 482 |
| Transformações | dbt-core / dbt-trino | 1.11 / 1.10 |
| Governança | Unity Catalog OSS | 0.4.0 |
| Observabilidade Spark | DataFlint | 0.9.9 |

---

## Documentação

| Arquivo | Descrição |
|---------|-----------|
| [`docs/nexomart_requisitos.md`](docs/nexomart_requisitos.md) | Requisitos funcionais e não-funcionais do NexoMart |
| [`docs/CICLO_01_CRONOGRAMA.md`](docs/CICLO_01_CRONOGRAMA.md) | Cronograma de vídeos do Ciclo 1 e roteiros |
| [`docs/ENGINEERING_DEBATE_AND_ROADMAP.md`](docs/ENGINEERING_DEBATE_AND_ROADMAP.md) | Decisões de engenharia e roadmap técnico |
| [`dbapi/walkthrough.md`](dbapi/walkthrough.md) | Walkthrough de implementação do domínio product |
