# Ciclo 1 — Apresentação do Canal e do Case
## Cronograma Detalhado de Vídeos + Case NexoMart

> **Legenda**: DT = Deep Track (8–12 min, fundamentos densos) | FT = Fast Track (15–25 min, hands-on)
> **Série**: "Data Platform do Zero — Construindo um Data Lakehouse Open-Source"

---

## PARTE I — O CASE: NexoMart

### 1.1 Contexto da Empresa

**NexoMart** é um marketplace brasileiro de médio porte com operação multi-canal (web, app, marketplace partners) que processa entre **100 mil e 50 milhões de pedidos por ciclo de dados** dependendo da sazonalidade (Black Friday, Natal, Dia das Mães).

A empresa opera com um banco OLTP PostgreSQL maduro, mas enfrenta um problema clássico: **o banco transacional não consegue responder às perguntas analíticas do negócio em tempo razoável** sem afetar a operação.

### 1.2 Domínios de Negócio

| Domínio | Schema | Descrição |
|---|---|---|
| Produto | `product` | Catálogo de produtos, modelos, categorias, descrições em vetores |
| Pessoa | `person` | Clientes, vendedores, endereços, contatos |
| Vendas | `sales` | Pedidos, itens (JSONB), devoluções, pagamentos |
| Marketing | `marketing` | Campanhas, anúncios, performance por canal |
| RH | `humanresources` | Funcionários, departamentos, histórico de cargos (SCD Type 2) |
| Compras | `purchasing` | Ordens de compra, fornecedores, recebimento |
| Estoque | `warehouse` | Inventário, transferências, histórico de movimentações |

### 1.3 Requisitos Funcionais do Case

#### RF-01: Ingestão de Dados Operacionais
- O sistema deve ingerir dados transacionais de todos os 7 domínios via API REST assíncrona (FastAPI + asyncpg)
- A ingestão deve suportar bulk insert para volumes de até 50M registros por execução
- A API deve implementar arquitetura 3-layer: Repository → Service → Router
- Todos os modelos devem implementar auditoria automática (created_at, updated_at, is_active, row_guid)

#### RF-02: Orquestração de Pipelines
- O sistema deve orquestrar todos os pipelines via Apache Airflow com DAGs declarativas
- Os seeders devem ser acionados via BashOperator como scripts CLI
- Volumes devem ser dinâmicos por execução (range min/max sorteado aleatoriamente)
- A taxa de chaos engineering deve variar entre 1% e 10% por execução

#### RF-03: Armazenamento Distribuído e Versionado
- Dados processados devem ser armazenados em tabelas Delta Lake no MinIO (object storage S3-compatible)
- A governança, catálogo e controle de acesso devem ser gerenciados pelo Unity Catalog OSS
- O formato de armazenamento deve suportar time-travel queries e rollback de dados

#### RF-04: Processamento Distribuído
- Transformações analíticas devem ser executadas via Apache Spark
- O Spark deve ler do PostgreSQL (OLTP) e escrever em Delta Lake (OLAP) via camadas Bronze/Silver/Gold
- O processamento deve suportar Structured Streaming para cenários near-real-time

#### RF-05: Federação de Consultas
- Analistas devem consultar dados via SQL padrão usando Apache Trino
- O Trino deve federar dados do Delta Lake (MinIO/Unity Catalog OSS), PostgreSQL e futuras fontes
- Queries analíticas devem retornar resultados em menos de 10s para volumes de 50M registros

#### RF-06: Qualidade de Dados e Chaos Engineering
- O sistema deve injetar anomalias controladas (duplicatas, dados sujos, fraudes simuladas) para testar pipelines de qualidade
- As anomalias devem ser documentadas em metadados para uso em testes de ML e auditoria
- O sistema deve suportar cenários de detecção de fraude, chargeback e dados incompletos

#### RF-07: Capacidades AI-Ready
- O domínio `product` deve suportar busca semântica via pgvector (embeddings de descrições)
- O sistema deve estar preparado para integração com modelos de linguagem para geração de insights
- A modelagem deve suportar futuras features de recomendação de produtos

### 1.4 Requisitos Não-Funcionais

| Requisito | Meta |
|---|---|
| Throughput de ingestão | 50M registros/execução |
| Latência analítica | < 10s para queries sobre 50M registros |
| Disponibilidade OLTP | 99.9% |
| Retenção de dados | Indefinida (Delta Lake + MinIO com time-travel nativo) |
| Rastreabilidade | 100% das operações com audit trail |
| Portabilidade | 100% open-source, zero custo de cloud proprietária |

### 1.5 Stack Tecnológica

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
│  Delta Lake + Unity Catalog OSS (governança) + MinIO (S3) │
├──────────────────────────────────────────────────────────┤
│               FEDERAÇÃO DE CONSULTAS                      │
│                     Apache Trino                          │
└──────────────────────────────────────────────────────────┘
```

---

## PARTE II — CRONOGRAMA DO CICLO 1

### Visão Geral do Ciclo

| # | Trilha | Tipo | Duração Est. | Tema |
|---|--------|------|--------------|------|
| 01 | DT | Apresentação | 10–12 min | Canal + Case NexoMart + Motivadores |
| 02 | DT | Fundamentos | 10–12 min | Modelo de Dados + Escolha do PostgreSQL |
| 03 | FT | Hands-on | 18–22 min | Modelagem Relacional — Tabelas, Constraints, Relacionamentos |
| 04 | FT | Hands-on | 18–22 min | Normalização, SCD Type 2 e pgvector |
| 05 | FT | Hands-on | 18–22 min | JSONB — Semi-estruturado sem Abrir Mão do ACID |
| 06 | FT | Hands-on | 18–22 min | Indexando JSONB — GIN, Expression Indexes e EXPLAIN ANALYZE |
| 07 | FT | Hands-on | 18–22 min | PostgreSQL Avançado — Particionamento, Window Functions e CTEs |
| 08 | FT | Hands-on | 15–20 min | pgvector e Full Text Search — PostgreSQL como Banco de IA |

---

### Vídeo 01 — DT: Apresentação do Canal e do Case NexoMart

**Objetivo**: Apresentar quem você é, qual o problema que o canal resolve e plantar o gancho para o próximo vídeo.

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook — "Esse pipeline aqui processa 50M registros. Tudo open-source,
                        tudo na sua máquina. Vamos construir do zero?"
00:30 - 01:30  Quem sou eu e por que esse canal existe
01:30 - 03:30  O problema: OLTP não é OLAP (dor que o espectador reconhece)
03:30 - 06:00  Apresentação do NexoMart — o case que vamos construir junto
06:00 - 08:30  Os 7 domínios de negócio e o que acontece em cada um
08:30 - 10:30  A stack completa — por que cada ferramenta foi escolhida
10:30 - 12:00  O que você vai aprender ao longo da série + CTA
```

**Pontos obrigatórios:**
- Mostrar o diagrama `data_platform_architecture.png` (visão geral da stack)
- Mostrar o `business_db.png` (esquema OLTP do NexoMart)
- Demonstrar brevemente o terminal com Docker Compose subindo para mostrar que funciona
- Frase de posicionamento do canal: *"Este canal é para engenheiros de dados que querem construir uma plataforma moderna, open-source e production-like do zero, sem depender de cloud proprietária."*

**Gancho para o Vídeo 02:**
> *"Mas antes de subir qualquer container, tem uma decisão que vai determinar se tudo isso funciona — a escolha do banco de dados e como os dados estão modelados. No próximo vídeo, eu te mostro por que escolhi o PostgreSQL e não MongoDB, Cassandra ou DynamoDB — e por que essa escolha importa mais do que você imagina."*

---

### Vídeo 02 — DT: Modelo de Dados e Escolha do Banco de Dados

**Objetivo**: Fundamentar tecnicamente por que PostgreSQL é a escolha certa para o OLTP do NexoMart e apresentar o modelo de dados.

**Estrutura de roteiro:**

```
00:00 - 00:30  Recap do vídeo anterior (15 seg)
00:30 - 02:00  O problema de escolher um banco de dados (não é sobre moda)
02:00 - 05:00  Comparativo: PostgreSQL vs alternativas
                - MongoDB: flexibilidade vs consistência (ACID)
                - Cassandra: escala vs complexidade operacional
                - DynamoDB: managed vs vendor lock-in e custo
05:00 - 08:00  Por que PostgreSQL vence no OLTP de marketplace
                - ACID nativo
                - JSONB (melhor dos dois mundos)
                - pgvector (AI-ready sem trocar de banco)
                - Particionamento nativo
                - Extensions ecosystem
08:00 - 11:00  O modelo de dados do NexoMart (business_db.png walkthrough)
                - Os 7 schemas e seus papéis
                - Relacionamentos entre domínios
                - Convenções: snake_case, AuditMixin, schema-qualified tables
11:00 - 12:00  Gancho para o próximo vídeo
```

**Pontos obrigatórios:**
- Tabela comparativa visual (PostgreSQL vs MongoDB vs Cassandra)
- Mostrar o `business_db.sql` — o DDL completo
- Explicar o `AuditMixin` (created_at, updated_at, is_active, row_guid)
- Mencionar que o pgvector aparece nos vídeos do produto mais à frente

**Gancho para o Vídeo 03:**
> *"Agora que você sabe por que escolhemos o PostgreSQL, no próximo vídeo a gente vai fundo na modelagem — linha a linha, constraint a constraint. Por que essa tabela tem JSONB? Por que esse relacionamento usa UUID em vez de BIGSERIAL? As decisões que parecem pequenas aqui vão fazer toda diferença quando o volume escalar."*

---

### Vídeo 03 — FT: Modelagem Relacional — Tabelas, Constraints e Relacionamentos

**Objetivo**: Walkthrough hands-on do modelo de dados — foco em decisões de design (não só DDL).

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook + Recap
00:30 - 02:00  O que vamos cobrir hoje (mapa visual dos schemas)
02:00 - 06:00  Schema product — ProductModel, ProductCategory, ProductModel
                - Por que UUID como PK (distribuído, sem sequência global)
                - Índices GIN no JSONB de atributos
                - O campo vector(1536) — spoiler do pgvector
06:00 - 10:00  Schema person — Customer, Address, Contact
                - FK com ON DELETE CASCADE vs SET NULL (trade-offs)
                - Normalização: por que endereço é uma tabela separada
                - Validação de CPF/CNPJ: constraint vs application logic
10:00 - 15:00  Schema sales — SalesOrder, SalesOrderItem (JSONB)
                - Por que order_items é JSONB e não uma tabela relacional
                - Prefixo de ID transacional: SO-XXXXXXXXX
                - Status com Enum tipado (Python + PostgreSQL)
15:00 - 18:00  Schema warehouse — Inventory, TransactionHistory
                - Particionamento por data (preview)
                - TR-XXXXXXXXX como prefixo
18:00 - 20:00  Demo: criando as tabelas no banco local + verificando com \d
20:00 - 22:00  Recap + gancho
```

**Pontos obrigatórios:**
- Mostrar o `base.py` com o `AuditMixin` completo
- Explicar a diferença entre `UUID` e `BIGSERIAL` em sistemas distribuídos
- Demonstrar a criação real das tabelas via `uv run python` ou Alembic

**Gancho para o Vídeo 04:**
> *"Mas tem dois tópicos que deixei pra amanhã de propósito — SCD Type 2, que é como o RH do NexoMart mantém o histórico completo de cada funcionário sem perder nada, e pgvector, a extensão que transforma o PostgreSQL num banco de dados de IA. Se você trabalha com dados e ainda não sabe o que é SCD Type 2, o próximo vídeo é obrigatório."*

---

### Vídeo 04 — FT: Normalização, SCD Type 2 e pgvector

**Objetivo**: Cobrir os dois tópicos avançados do modelo — controle de histórico e AI-readiness.

**Estrutura de roteiro:**

```
00:00 - 00:30  Recap
00:30 - 03:00  O problema do histórico: tabelas que só guardam o presente
                - Exemplo: funcionário muda de cargo. O que acontece com o registro antigo?
03:00 - 08:00  SCD Type 2 no humanresources
                - valid_from / valid_to / is_current
                - Como o INSERT + UPDATE funciona
                - Query para pegar o estado em qualquer ponto do tempo
                - Comparação: SCD1 vs SCD2 vs SCD3 vs SCD4
08:00 - 12:00  pgvector — o PostgreSQL como banco vetorial
                - O que é um embedding (analogia com coordenadas)
                - Instalação e habilitação da extensão
                - O campo `vector(1536)` no ProductModel
                - Operadores: <-> (L2), <#> (inner product), <=> (cosine)
12:00 - 17:00  Demo: gerando embeddings com OpenAI/local model e inserindo no PG
                - Query de similaridade: "produtos parecidos com este"
                - Criando índice HNSW para buscas em escala
17:00 - 20:00  Por que isso é importante para o futuro da plataforma
                - Recomendação de produtos
                - Busca semântica no catálogo
                - RAG sobre dados do NexoMart
20:00 - 22:00  Recap + gancho para modelagem dimensional
```

**Gancho para o Vídeo 05 (série JSONB):**
> *"Nos próximos vídeos vamos fundo em um recurso específico do PostgreSQL que eu acho que é o mais subestimado da comunidade de dados: o JSONB. Não é só uma coluna JSON — é um tipo nativo indexável que muda completamente como você projeta certas tabelas. Vem comigo."*

---

### Vídeo 05 — FT: JSONB — Semi-Estruturado sem Abrir Mão do ACID

**Objetivo**: Apresentar JSONB como recurso de design, não apenas como "coluna JSON".

**Por que 3+ vídeos de PostgreSQL?**
O PostgreSQL tem features que justificam cobertura dedicada porque são diferenciais reais para engenheiros de dados. Muitos profissionais conhecem SQL básico mas não exploram o potencial analítico nativo do banco — esses vídeos posicionam o canal como referência técnica de profundidade.

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook: "Essa query aqui filtra por dentro de um campo JSON com índice.
                       Tempo de resposta: 12ms. 2 milhões de registros. Sem ORM."
00:30 - 02:00  json vs jsonb — por que jsonb é sempre a escolha
                - json: texto, preserva formatação, não indexável eficientemente
                - jsonb: binário, deduplication de chaves, indexável com GIN
02:00 - 06:00  Quando usar JSONB (e quando NÃO usar)
                - ✅ Atributos variáveis por categoria de produto
                - ✅ Items de pedido (evitar 1:N desnecessário para leitura)
                - ✅ Metadados de eventos (audit logs, telemetria)
                - ❌ Campos que você filtra frequentemente em queries críticas
                - ❌ Campos que precisam de FK constraints
                - ❌ Quando a estrutura é 100% conhecida e estável
06:00 - 10:00  Operadores fundamentais (com demo ao vivo)
                - -> e ->> (acesso por chave/índice)
                - #> e #>> (acesso por path)
                - @> e <@ (containment)
                - ? e ?| e ?& (key existence)
                - jsonb_each() e jsonb_array_elements()
                - jsonb_build_object() e jsonb_agg()
10:00 - 15:00  JSONB no NexoMart: order_items
                - Estrutura real da coluna no SalesOrderItem
                - Inserindo pedidos com JSONB
                - Consultando: "total de itens de uma categoria no pedido"
                - Expandindo JSONB em linhas com jsonb_array_elements
15:00 - 18:00  JSONB no NexoMart: product attributes
                - Atributos variáveis por categoria (roupas: tamanho/cor, eletrônicos: voltagem/peso)
                - Filtro por atributo: WHERE attributes @> '{"cor": "azul"}'
18:00 - 20:00  Recap + gancho para índices JSONB
```

**Gancho para o Vídeo 06:**
> *"O problema é que sem o índice certo, essa query vai fazer um full table scan de 2 milhões de linhas toda vez. No próximo vídeo a gente vai resolver isso com GIN index — e eu vou te mostrar no EXPLAIN ANALYZE a diferença de uma query que leva 4 segundos para uma que leva 12 milissegundos."*

---

### Vídeo 06 — FT: Indexando JSONB — GIN, Expression Indexes e EXPLAIN ANALYZE

**Objetivo**: Mostrar o impacto real de indexação correta em JSONB com evidência visual via EXPLAIN ANALYZE.

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook: EXPLAIN ANALYZE mostrando Seq Scan vs Bitmap Index Scan
00:30 - 02:00  Recap de JSONB + por que indexação é crítica
02:00 - 06:00  GIN Index — como funciona por dentro
                - Inverted index: mapeia valores → linhas
                - jsonb_ops vs jsonb_path_ops
                  - jsonb_ops: suporta ?, ?|, ?&, @>, @?
                  - jsonb_path_ops: só @>, mais compacto e rápido para containment
                - Custo de escrita vs ganho de leitura
06:00 - 10:00  Demo: criando e comparando GIN indexes
                - CREATE INDEX gin_order_items ON sales.sales_order USING GIN (order_items);
                - EXPLAIN ANALYZE antes e depois
                - Medir: planning time, execution time, heap fetches
10:00 - 14:00  Expression Indexes em JSONB
                - Quando usar: campo específico acessado frequentemente
                - CREATE INDEX ON product.product ((attributes->>'categoria'));
                - Partial indexes: WHERE attributes->>'status' = 'ativo'
                - Comparação de tamanho: GIN completo vs expression index
14:00 - 18:00  BRIN Index para colunas temporais
                - Ideal para tabelas particionadas por data (sales, warehouse)
                - Como funciona: min/max por bloco físico
                - Custo mínimo de storage, excelente para time-series
18:00 - 20:00  Guia de decisão: qual índice usar?
                | Caso de uso                    | Índice recomendado    |
                |---                             |---                    |
                | JSONB containment (@>)          | GIN jsonb_path_ops   |
                | JSONB key existence (?)         | GIN jsonb_ops        |
                | Campo específico do JSONB       | Expression Index     |
                | Coluna timestamp em fact table  | BRIN                 |
                | Texto com like %termo%          | GIN + pg_trgm        |
                | Coluna de alta cardinalidade    | B-Tree (padrão)      |
20:00 - 22:00  Recap + gancho
```

**Gancho para o Vídeo 07:**
> *"Agora que você sabe indexar JSONB, vamos para os dois recursos do PostgreSQL que analistas de dados mais precisam mas menos conhecem: Window Functions e Particionamento de tabelas. Window Functions vão substituir subqueries que você escreve hoje — e são até 10x mais rápidas. Vem."*

---

### Vídeo 07 — FT: PostgreSQL Avançado — Particionamento, Window Functions e CTEs

**Objetivo**: Cobrir recursos analíticos nativos do PostgreSQL que aproximam OLTP de OLAP.

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook
00:30 - 02:00  Contexto: PG como banco híbrido OLTP + OLAP leve
02:00 - 07:00  Particionamento de Tabelas
                - Range Partitioning: sales_order por order_date (por mês/ano)
                - List Partitioning: warehouse por região/hub
                - Partition Pruning: como o planner ignora partições irrelevantes
                - Demo: EXPLAIN para query com e sem pruning
                - Constraint Exclusion + declarative partitioning (PG 10+)
07:00 - 13:00  Window Functions — SQL Analítico sem Subquery
                - Sintaxe: OVER (PARTITION BY ... ORDER BY ...)
                - ROW_NUMBER() — ranking de pedidos por cliente
                - RANK() e DENSE_RANK() — top produtos por categoria
                - LAG() e LEAD() — variação de receita mês a mês
                - SUM() OVER — receita acumulada (running total)
                - NTILE() — segmentação de clientes em quartis (RFM preview)
                - Demo ao vivo com dados do NexoMart
13:00 - 17:00  CTEs — Common Table Expressions
                - WITH clause: legibilidade vs performance
                - CTEs recursivas: hierarquia de categorias de produto
                - Materialized vs não-materialized (PG 12+)
                - Quando CTE é melhor que subquery (e quando não é)
17:00 - 20:00  Materialized Views
                - Caso de uso: agregação de receita por dia já calculada
                - REFRESH MATERIALIZED VIEW CONCURRENTLY
                - Limitação: não é incremental (preview: isso é o que o dbt resolve)
20:00 - 22:00  Recap + gancho duplo
```

**Gancho para Vídeo 08 (pgvector) e para Modelagem Dimensional:**
> *"No próximo vídeo fechamos o ciclo de PostgreSQL com pgvector e Full Text Search — IA de verdade dentro do banco. E depois disso, entramos na modelagem dimensional: Star Schema, fatos, dimensões, grain — onde o dado deixa de ser operacional e vira analítico de verdade. Esse é o coração de toda plataforma de dados."*

---

### Vídeo 08 — FT: pgvector e Full Text Search — PostgreSQL como Banco de IA

**Objetivo**: Mostrar que o PostgreSQL pode servir como banco vetorial e de busca semântica sem adicionar infraestrutura.

**Estrutura de roteiro:**

```
00:00 - 00:30  Hook: "Busca semântica em produto sem Elasticsearch, sem Pinecone,
                       sem mais nenhum serviço. Só PostgreSQL."
00:30 - 03:00  O problema: busca por palavra-chave vs busca por significado
                - "tênis para corrida" não retorna "calçado esportivo para atletismo"
                - Onde Full Text Search resolve e onde precisa de vetores
03:00 - 08:00  Full Text Search nativo
                - tsvector e tsquery
                - to_tsvector('portuguese', nome || ' ' || descricao)
                - GIN index em tsvector
                - ts_rank() para ranking de relevância
                - Configuração de dicionário pt_BR
                - Demo: busca por termo no catálogo NexoMart
08:00 - 12:00  pg_trgm — busca fuzzy para erros de digitação
                - Trigram similarity (%)
                - GIN index com pg_trgm
                - Caso de uso: "sugestão de produto" enquanto digita
12:00 - 18:00  pgvector — embeddings no banco de dados
                - O que é um embedding (coordenadas no espaço semântico)
                - Instalação: CREATE EXTENSION vector
                - O campo vector(1536) no ProductModel
                - Gerando embeddings: API OpenAI ou modelo local (nomic-embed)
                - Inserindo embeddings: UPDATE product SET embedding = '[0.1, 0.2, ...]'
                - Operadores de distância: <-> L2, <#> inner product, <=> cosine
                - IVFFlat vs HNSW index (trade-off: build time vs query time)
                - Demo: "produtos similares a este" com cosine similarity
18:00 - 20:00  Quando usar cada abordagem
                | Caso de uso                       | Solução               |
                |---                                |---                    |
                | Busca por palavra exata            | ILIKE / B-Tree       |
                | Busca por termo com variações      | Full Text Search     |
                | Tolerância a erros de digitação    | pg_trgm              |
                | Busca por significado/contexto     | pgvector             |
                | Tudo junto                         | Hybrid search        |
20:00 - 22:00  Gancho para Ciclo 2 (Modelagem da API) + Dimensional
```

**Gancho para o Ciclo 2:**
> *"Agora que o banco está modelado, indexado e AI-ready, o próximo ciclo começa com a API — a camada que alimenta tudo isso. FastAPI assíncrono, SQLAlchemy 2.0, arquitetura 3-layer, e como gerar dados em volume com chaos engineering para simular um ambiente real. Até lá."*

---

## PARTE III — ETAPAS DE DESENVOLVIMENTO

### Fase 1: Fundação (paralela ao Ciclo 1 de vídeos)

**Objetivo**: Ter a base funcional para demonstrar nos vídeos 01–04.

| Tarefa | Artefatos | Status |
|---|---|---|
| Schema OLTP completo | `business_db.sql`, `business_db.png` | ✅ Pronto |
| Diagrama de arquitetura | `data_platform_architecture.png` | ✅ Pronto |
| Modelos SQLAlchemy (7 domínios) | `dbapi/api/models/` | ✅ Pronto |
| AuditMixin e Base | `dbapi/api/config/base.py` | ✅ Pronto |
| Configuração Docker Compose (todos os serviços) | `docker-compose.yml` por serviço | ✅ Pronto |
| Seeders com chaos engineering | `dbapi/api/seeder/` | ✅ Pronto |
| Arquitetura 3-layer (Repository/Service/Router) | `dbapi/api/` | ✅ Pronto |

### Fase 2: PostgreSQL Features (para vídeos 05–08)

**Objetivo**: Criar demos reproduzíveis para os vídeos de features do PG.

| Tarefa | Artefatos | Prioridade |
|---|---|---|
| Script de setup do pgvector | `demos/pgvector_setup.sql` | Alta |
| Notebook de embeddings com modelo local | `demos/embeddings_demo.ipynb` | Alta |
| Scripts de JSONB com EXPLAIN ANALYZE | `demos/jsonb_benchmarks.sql` | Alta |
| Script de particionamento (sales por mês) | `demos/partitioning_demo.sql` | Alta |
| Scripts de Window Functions com dados reais | `demos/window_functions.sql` | Alta |
| Script de Full Text Search pt_BR | `demos/fts_demo.sql` | Média |
| Script de GIN indexes comparativo | `demos/gin_index_benchmark.sql` | Alta |
| Seeder específico para volume de demo (100k registros) | `demos/demo_seeder.py` | Alta |

### Fase 3: Modelagem Dimensional (Ciclo 2 — próximo)

**Objetivo**: Construir o Data Warehouse dimensional sobre o OLTP do NexoMart.

| Tarefa | Descrição |
|---|---|
| Bus Matrix do NexoMart | 5 fatos × 9 dimensões mapeadas |
| Star Schema diagrama | Visual para usar no EP01 do Ciclo 2 |
| Tabelas Fato | fact_sales, fact_inventory, fact_marketing_performance |
| Dimensões | dim_product, dim_customer, dim_date, dim_location, dim_seller |
| Slowly Changing Dimensions | dim_employee com SCD2 (herda do OLTP) |
| Grain declaration | 1 linha por order_item (mais granular possível) |

---

## PARTE IV — FEATURES DO POSTGRESQL: GUIA DE COBERTURA

### Mapa completo de features por vídeo

| Feature | Vídeo | Profundidade |
|---|---|---|
| JSONB vs JSON | 05 | Conceitual + prático |
| Operadores JSONB (->, ->>, @>, ?) | 05 | Hands-on |
| Funções JSONB (jsonb_each, jsonb_agg) | 05 | Hands-on |
| GIN Index (jsonb_ops vs jsonb_path_ops) | 06 | Profundo |
| Expression Index em JSONB | 06 | Profundo |
| BRIN Index (time-series) | 06 | Conceitual |
| EXPLAIN ANALYZE | 06 | Profundo |
| Particionamento (Range, List) | 07 | Profundo |
| Partition Pruning | 07 | Demo |
| Window Functions (ROW_NUMBER, LAG, SUM OVER) | 07 | Profundo |
| CTEs e CTEs Recursivas | 07 | Profundo |
| Materialized Views | 07 | Conceitual |
| Full Text Search (tsvector, tsquery, GIN) | 08 | Profundo |
| pg_trgm (fuzzy search) | 08 | Hands-on |
| pgvector (embeddings, IVFFlat, HNSW) | 04 + 08 | Profundo |
| SCD Type 2 (valid_from, valid_to) | 04 | Profundo |
| Enum types (Python + PostgreSQL) | 03 | Conceitual |
| UUID vs BIGSERIAL | 03 | Conceitual |
| CHECK e UNIQUE constraints | 03 | Hands-on |
| Schema-qualified tables | 03 | Conceitual |

### Features reservadas para ciclos futuros

| Feature | Ciclo sugerido | Motivo |
|---|---|---|
| LISTEN / NOTIFY | Ciclo 3 (Kafka) | Comparativo com mensageria |
| Row Level Security | Ciclo 4 (Auth) | Contextualizado com multi-tenant |
| pg_partman (automação de partições) | Ciclo 3 | Depois de particionamento básico |
| Logical Replication / CDC | Ciclo 3 (Debezium) | Junto com Outbox Pattern |
| pg_cron | Ciclo 2 (Airflow) | Comparativo com orquestração |
| Vacuuming e bloat management | Ciclo futuro (Ops) | DBA track |

---

## PARTE V — HOOKS E GANCHOS ENTRE CICLOS

### Ciclo 1 → Ciclo 2 (Modelagem da API)

O Ciclo 1 termina com o banco modelado e documentado. O gancho natural para o Ciclo 2 é:

> *"O banco está pronto. Agora a pergunta é: como inserir 50 milhões de registros sem travar tudo? No Ciclo 2, construímos a camada de ingestão: FastAPI assíncrono, SQLAlchemy 2.0, arquitetura 3-layer e seeders com chaos engineering."*

### Ciclo 2 → Ciclo 3 (Modelagem Dimensional)

> *"Os dados estão no banco OLTP. Mas o OLTP não foi feito para analytics. Agora vamos construir o modelo dimensional — Star Schema, fatos, dimensões, e o primeiro pipeline ETL com Spark e Delta Lake."*

---

*Documento criado em 2026-04-28 | Projeto: Data Platform do Zero — NexoMart*
