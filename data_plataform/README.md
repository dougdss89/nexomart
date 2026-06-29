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
│  Landing → Bronze → Silver → Gold  (Delta Lake / MinIO)      │
├──────────────────────────────────────────────────────────────┤
│                  ARMAZENAMENTO ANALÍTICO                      │
│       Delta Lake (formato) + MinIO (S3-compatible)           │
├──────────────────────────────────────────────────────────────┤
│                  CATÁLOGO & METASTORE                         │
│    Hive Metastore 4.0 (PostgreSQL) + Unity Catalog OSS       │
├──────────────────────────────────────────────────────────────┤
│                   FEDERAÇÃO DE CONSULTAS                      │
│              Apache Trino 482 (conector delta_lake)           │
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
 Landing (Parquet bruto, s3a://landing/)
      │  spark/jobs/landing_layer_scripts/
      ▼
 Bronze (Delta Lake, s3a://bronze/)
      │  spark/jobs/raw_layer_extract_scripts/
      ▼
 Silver (Delta Lake + surrogate keys, s3a://silver/)
      │  spark/jobs/silver_layer_advworks_scripts/
      ▼
 Gold (Delta Lake, s3a://deltadw/gold.db/)
      │  spark/jobs/gold_layer_scripts/
      ▼
 Trino → dbt (views/marts analíticos)
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
├── airflow/
│   ├── dags/               # DAGs do Airflow + .env de paths
│   └── dockerfile          # Astro Runtime 11.4 com Spark embutido
├── spark/
│   ├── jobs/               # Scripts PySpark por camada
│   │   ├── landing_layer_scripts/
│   │   ├── raw_layer_extract_scripts/    # bronze
│   │   ├── silver_layer_advworks_scripts/
│   │   └── gold_layer_scripts/
│   └── conf/
│       └── spark-defaults.conf.template  # gerado via envsubst
├── hive/                   # Hive Metastore 4.0 (PostgreSQL backend)
├── trino/                  # Federação SQL (conector delta_lake)
├── unitycatalog/           # Unity Catalog OSS (governança)
├── configurations/
│   ├── .env                # Paths de jobs, buckets MinIO, conexões
│   ├── secrets.env         # Credenciais (não commitado)
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

## Configurações Críticas da Stack

### spark-defaults.conf

Gerado pelo `make up` via `envsubst` a partir do template. **Nunca edite o `.conf` diretamente** — edite o `.template` e regenere:

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

Chaves obrigatórias para a stack funcionar:

```properties
# Catálogo externo — sem isso Spark usa Derby local (invisível ao Trino)
spark.sql.catalogImplementation      hive
spark.sql.warehouse.dir              s3a://deltadw/warehouse/
spark.hadoop.hive.metastore.uris     thrift://hive-metastore:9083

# Credenciais MinIO — devem vir do secrets.env via envsubst
spark.hadoop.fs.s3a.access.key       minioadmin
spark.hadoop.fs.s3a.secret.key       minioadmin
spark.hadoop.fs.s3a.endpoint         http://minio:9000
```

### Paths dos Jobs Spark no Airflow

Definidos em `configurations/.env`. Os containers Airflow montam `spark/jobs/` do host via bind mount direto em `/usr/local/airflow/spark_jobs/`:

```
AIRFLOW_SPARK_JOBS_FOLDER_LANDING_SCRIPT=/usr/local/airflow/spark_jobs/landing_layer_scripts
AIRFLOW_SPARK_JOBS_FOLDER_RAW_SCRIPT=/usr/local/airflow/spark_jobs/raw_layer_extract_scripts
AIRFLOW_SPARK_JOBS_FOLDER_SILVER_SCRIPT=/usr/local/airflow/spark_jobs/silver_layer_advworks_scripts
AIRFLOW_SPARK_JOBS_FOLDER_GOLD_SCRIPT=/usr/local/airflow/spark_jobs/gold_layer_scripts
```

Qualquer script editado em `spark/jobs/` fica imediatamente disponível nos containers Airflow — sem `docker cp`.

### Gold Layer — escrita Delta

Os jobs da camada Gold usam `mode("overwrite")` em vez de `DROP + saveAsTable`. Isso garante que o Delta Log avance de versão (v0→v1→v2…) em vez de ser apagado e recriado do zero a cada run, o que evitaria o cache de metadados do Trino apontar para arquivos Parquet deletados.

```python
dataframe.write \
    .format("delta") \
    .option("path", table_path) \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save()

# Registrar no Hive só se ainda não existir
if not spark.catalog.tableExists(full_table):
    spark.sql(f"CREATE TABLE {full_table} USING delta LOCATION '{table_path}'")
```

### dbt — Modelos Gold

Marts analíticos em `configurations/dbt/models/gold/`. Atenção:

- Use `materialized='view'` para marts que são projeções diretas de tabelas Gold. Views são armazenadas **apenas no Hive Metastore** (sem dados no MinIO), são idempotentes e não acumulam resíduos `__dbt_tmp` / `__dbt_backup` no S3.
- Evite `materialized='table'` com Trino Delta Lake sem uma estratégia explícita de limpeza: o rename atômico do dbt-trino deixa diretórios `__dbt_tmp-<uuid>` e `__dbt_backup` no MinIO sempre que algum passo falha.

---

## DAG de Integração End-to-End

`integration_e2e_test` — sequência completa do pipeline:

```
start → seed_postgres → pg_landing → pg_bronze → pg_silver
      → pg_gold → trino_validate → dbt_run → dbt_test → end
```

Para disparar:

```bash
# Via CLI do Airflow (dentro do scheduler)
docker exec airflow_scheduler airflow dags trigger integration_e2e_test

# Monitorar resultado
docker exec airflow_scheduler airflow tasks states-for-dag-run \
  integration_e2e_test <run_id>
```

---

## Problemas Conhecidos e Correções

### 1. Spark usa Derby local em vez do Hive Metastore externo

**Sintoma:** tabelas criadas pelo Spark não aparecem no Trino; diretório `spark-warehouse/` aparece dentro do container.

**Causa:** `spark.sql.catalogImplementation` não configurado — padrão é `in-memory` (Derby embarcado).

**Correção:** garantir no `spark-defaults.conf`:
```properties
spark.sql.catalogImplementation = hive
```

---

### 2. Trino: `HIVE_CANNOT_OPEN_SPLIT` após recriar tabela Gold

**Sintoma:** `SELECT *` na tabela Gold falha com `Error opening Hive split s3a://.../<uuid>.parquet`. `SELECT count(*)` passa (usa estatísticas do Delta Log sem abrir Parquet).

**Causa:** o Trino cacheia o conteúdo do `_delta_log/00000000000000000000.json` em memória. Se a tabela é apagada e recriada na mesma path, o Delta Log reinicia no version 0 (mesmo nome de arquivo), mas o Trino serve o conteúdo antigo — que aponta para arquivos Parquet já deletados.

**Correção aplicada:** a camada Gold agora usa `mode("overwrite")` em vez de `DROP + recreate`, criando novas versões do Delta Log (v1, v2…). O Trino lê o arquivo de commit mais recente (nome diferente) sem conflito de cache.

**Correção emergencial** (se o cache já estiver corrompido):
```bash
# Reiniciar Trino limpa o cache em memória
docker restart trino-coordinator trino-trino-worker-1

# Ou via procedure (sem restart):
docker exec trino-coordinator trino \
  --execute "CALL delta.system.flush_metadata_cache(schema_name => 'gold', table_name => '<tabela>')" \
  --server http://localhost:8080 --user admin
```

---

### 3. Hive Metastore 4.0: Thrift timeout em `createTable`

**Sintoma:** log Spark mostra `RetryingMetaStoreClient: MetaStoreClient lost connection. createTable` com `TTransportException: java.net.SocketTimeoutException: Read timed out`. Jobs da camada Gold falham após ~1 min.

**Causa:** Hive 4.0 deixa transações PostgreSQL em estado `idle in transaction` quando o cliente Thrift faz timeout. Essas transações bloqueiam DDL (como `ALTER TABLE TBL_PRIVS ADD CONSTRAINT FK1`), que por sua vez bloqueia novos `createTable`.

**Correção aplicada:**

1. `hive/conf/hive-site.xml` — ampliar pool de threads do servidor Thrift:
```xml
<property>
  <name>hive.metastore.server.max.threads</name>
  <value>200</value>
</property>
<property>
  <name>hive.metastore.client.socket.timeout</name>
  <value>120</value>
</property>
```

2. PostgreSQL — auto-kill de sessões travadas no banco `metastore`:
```sql
ALTER DATABASE metastore SET idle_in_transaction_session_timeout = '2min';
```

**Diagnóstico** (quando suspeitar de bloqueio):
```bash
docker exec pgprojeto psql -U admin -d metastore -c "
SELECT pid, state, now() - query_start AS duracao, query
FROM pg_stat_activity
WHERE datname = 'metastore' AND state = 'idle in transaction'
ORDER BY duracao DESC;"

-- Matar sessões travadas manualmente:
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'metastore'
  AND state = 'idle in transaction'
  AND now() - query_start > interval '1 minute';
```

---

### 4. WSL2: bind mount stale após `docker restart`

**Sintoma:** `docker restart hive-metastore` falha com `OCI runtime create failed: unable to start container process: error mounting "/run/desktop/mnt/host/wsl/docker-desktop-bind-mounts/Ubuntu-22.04/<hash>..."`.

**Causa:** Docker Desktop no WSL2 usa hashes para bind mounts; um `restart` tenta reusar o hash antigo que não existe mais.

**Correção:**
```bash
docker rm hive-metastore
docker compose -f hive/docker-compose.yml up -d
```

---

### 5. dbt: `DELTA_LAKE_INVALID_SCHEMA — no commits`

**Sintoma:** `dbt run` falha com `Delta table gold.gold_mart_productcategory has no commits`.

**Causa:** o Trino encontra um diretório `_delta_log/` no S3 de uma tentativa anterior interrompida (sem nenhum commit válido dentro), e não consegue ler nem dropar a tabela normalmente.

**Correção:**
```bash
# Forçar drop via Trino (ignora o estado inválido do Delta Log)
docker exec trino-coordinator trino \
  --execute "DROP TABLE IF EXISTS delta.gold.gold_mart_productcategory" \
  --server http://localhost:8080 --user admin

# Limpar o path no MinIO manualmente se o DROP não remover o S3
docker exec minio-minio-1 bash -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc rm --recursive --force local/deltadw/gold.db/gold_mart_productcategory/"
```

---

### 6. Airflow worker com `SQL_ALCHEMY_CONN` vazio (role "astro" does not exist)

**Sintoma:** `airflow_worker` crasha com `FATAL: role "astro" does not exist`. O `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` aparece como `postgresql+psycopg2://:@pgprojeto:5432/airflowdb` (user/password vazios).

**Causa:** o docker-compose do Airflow usa variáveis de ambiente no formato `${POSTGRES_USER}` que precisam ser resolvidas pelo Docker Compose durante a criação do container. Se o container for recriado via `docker compose` **sem** o flag `--env-file`, as variáveis não são expandidas.

**Correção:** sempre usar o Makefile para operar o Airflow:
```bash
make up-airflow   # usa: docker compose --env-file configurations/secrets.env
```

Nunca rodar `docker compose -f airflow/docker-compose.yaml up -d` diretamente sem exportar as variáveis antes.

---

### 7. Resíduos `__dbt_tmp` e `__dbt_backup` no MinIO

**Sintoma:** diretórios como `gold_mart_productcategory__dbt_tmp-<uuid>/` e `gold_mart_productcategory__dbt_backup/` acumulam no MinIO após runs com falha.

**Causa:** o `materialized='table'` do dbt-trino usa rename atômico em 3 passos; qualquer falha intermediária deixa paths órfãos no S3.

**Limpeza:**
```bash
# Via Trino (remove Hive + S3 para tabelas managed)
docker exec trino-coordinator trino --server http://localhost:8080 --user admin \
  --execute "DROP TABLE IF EXISTS delta.gold.gold_mart_productcategory__dbt_backup"

# Paths que não estão no Hive: remover direto no MinIO
docker exec minio-minio-1 bash -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin
  mc ls local/deltadw/gold.db/
  mc rm --recursive --force local/deltadw/gold.db/gold_mart_productcategory__dbt_tmp-<uuid>/"
```

**Prevenção:** usar `materialized='view'` para marts que são projeções diretas — sem escrita no S3, sem resíduos.

---

## Pontos de Atenção Operacional

### `make clean` — o que é destruído e o que sobrevive

| Recurso | `make down` | `make clean` |
|---|---|---|
| Containers | ✓ removidos | ✓ removidos |
| Volume `dockervol` (logs Airflow) | preservado | **destruído** |
| Volume `spark-logs` | preservado | **destruído** |
| Volume `spark-ivy-cache` (JARs) | preservado | **destruído** |
| Volume `spark-jobs` | preservado | preservado |
| Bind mount `spark/jobs/` (host) | preservado | preservado |
| Bind mount `configurations/` (host) | preservado | preservado |
| Dados MinIO (Delta Lake, Parquet) | preservado | preservado |
| Dados PostgreSQL | preservado | preservado |
| Rede `hadoopcluster` | preservado | **destruída** |

Após `make clean` + `make up`, os scripts Spark em `spark/jobs/` estão imediatamente disponíveis nos containers Airflow via bind mount — nenhuma cópia manual necessária.

### Ordem de dependência dos serviços

```
postgres → minio → hive-metastore → spark → trino → airflow → dbapi
```

O Hive Metastore precisa do PostgreSQL. O Spark precisa do Hive e do MinIO. O Trino precisa do Hive. O Airflow depende de todos para executar jobs. `make up` já respeita essa ordem.

### Regenerar spark-defaults.conf

Necessário após editar o template ou após `make clean`:

```bash
export $(grep -v '^#' configurations/secrets.env | xargs)
envsubst < spark/conf/spark-defaults.conf.template > spark/conf/spark-defaults.conf
```

O `make up` chama `generate-confs` automaticamente, que faz isso com o `--env-file` correto.

### Verificar saúde do Hive Metastore

```bash
# Sessões PostgreSQL do metastore (monitorar idle in transaction)
docker exec pgprojeto psql -U admin -d metastore -c \
  "SELECT state, count(*) FROM pg_stat_activity WHERE datname='metastore' GROUP BY state;"

# Tabelas registradas no Hive
docker exec -i pgprojeto psql -U admin -d metastore -c \
  'SELECT "TBL_NAME", "TBL_TYPE" FROM "TBLS" ORDER BY "TBL_NAME";'
```

### Verificar tabelas no Trino

```bash
docker exec trino-coordinator trino \
  --execute "SHOW TABLES IN delta.gold" \
  --server http://localhost:8080 --user admin

docker exec trino-coordinator trino \
  --execute "SELECT count(*) FROM delta.gold.gold_dim_productcategory" \
  --server http://localhost:8080 --user admin
```

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
| [`dbapi/walkthrough.md`](dbapi/walkthrough.md) | Walkthrough de implementação do domínio product |

---

## Stack Completa

| Componente | Tecnologia | Versão |
|---|---|---|
| API de Ingestão | FastAPI + asyncpg | — |
| ORM | SQLAlchemy | 2.0 |
| Validação | Pydantic | 2.x |
| Banco OLTP | PostgreSQL | 16.13 |
| Busca Semântica | pgvector | — |
| Object Storage | MinIO | 2025-09 |
| Orquestração | Apache Airflow (Astro Runtime) | 2.9.1 |
| Processamento | Apache Spark | 3.5.8 |
| Formato Analítico | Delta Lake | — |
| Metastore | Hive Metastore | 4.0.0 |
| Federação SQL | Apache Trino | 482 |
| Transformações | dbt-core / dbt-trino | 1.11 / 1.10 |
| Governança | Unity Catalog OSS | — |
