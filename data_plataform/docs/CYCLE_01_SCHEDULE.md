# Cycle 1 — Channel and Case Presentation
## Detailed Video Schedule + NexoMart Case

> **Legend**: DT = Deep Track (8–12 min, dense fundamentals) | FT = Fast Track (15–25 min, hands-on)
> **Series**: "Data Platform From Scratch — Building an Open-Source Data Lakehouse"

---

## PART I — THE CASE: NexoMart

### 1.1 Company Context

**NexoMart** is a mid-size Brazilian marketplace with multi-channel operations (web, app, marketplace partners) that processes between **100 thousand and 50 million orders per data cycle** depending on seasonality (Black Friday, Christmas, Mother's Day).

The company runs on a mature PostgreSQL OLTP database, but faces a classic problem: **the transactional database can't answer the business's analytical questions in a reasonable time** without affecting operations.

### 1.2 Business Domains

| Domain | Schema | Description |
|---|---|---|
| Product | `product` | Product catalog, models, categories, vector descriptions |
| Person | `person` | Customers, sellers, addresses, contacts |
| Sales | `sales` | Orders, items (JSONB), returns, payments |
| Marketing | `marketing` | Campaigns, ads, per-channel performance |
| HR | `humanresources` | Employees, departments, role history (SCD Type 2) |
| Purchasing | `purchasing` | Purchase orders, suppliers, receiving |
| Warehouse | `warehouse` | Inventory, transfers, movement history |

### 1.3 Case Functional Requirements

#### FR-01: Operational Data Ingestion
- The system must ingest transactional data from all 7 domains via an asynchronous REST API (FastAPI + asyncpg)
- Ingestion must support bulk insert for volumes up to 50M records per run
- The API must implement a 3-layer architecture: Repository → Service → Router
- All models must implement automatic auditing (created_at, updated_at, is_active, row_guid)

#### FR-02: Pipeline Orchestration
- The system must orchestrate every pipeline via Apache Airflow with declarative DAGs
- Seeders must be triggered via BashOperator as CLI scripts
- Volumes must be dynamic per run (min/max range drawn at random)
- The chaos engineering rate must vary between 1% and 10% per run

#### FR-03: Distributed, Versioned Storage
- Processed data must be stored in Delta Lake tables on MinIO (S3-compatible object storage)
- Governance, catalog, and access control must be managed by the Hive Metastore
- The storage format must support time-travel queries and data rollback

#### FR-04: Distributed Processing
- Analytical transformations must run via Apache Spark
- Spark must read from PostgreSQL (OLTP) and write to Delta Lake (OLAP) through Bronze/Silver/Gold layers
- Processing must support Structured Streaming for near-real-time scenarios

#### FR-05: Query Federation
- Analysts must query data via standard SQL using Apache Trino
- Trino must federate data across Delta Lake (MinIO/Hive Metastore), PostgreSQL, and future sources
- Analytical queries must return results in under 10s for volumes of 50M records

#### FR-06: Data Quality and Chaos Engineering
- The system must inject controlled anomalies (duplicates, dirty data, simulated fraud) to test quality pipelines
- Anomalies must be documented in metadata for use in ML testing and auditing
- The system must support fraud detection, chargeback, and incomplete-data scenarios

#### FR-07: AI-Ready Capabilities
- The `product` domain must support semantic search via pgvector (description embeddings)
- The system must be ready for integration with language models for insight generation
- The data model must support future product recommendation features

### 1.4 Non-Functional Requirements

| Requirement | Target |
|---|---|
| Ingestion throughput | 50M records/run |
| Analytical latency | < 10s for queries over 50M records |
| OLTP availability | 99.9% |
| Data retention | Indefinite (Delta Lake + MinIO with native time-travel) |
| Traceability | 100% of operations with an audit trail |
| Portability | 100% open-source, zero proprietary cloud cost |

### 1.5 Technology Stack

```
┌──────────────────────────────────────────────────────────┐
│                    INGESTION & API                        │
│  FastAPI (async) + SQLAlchemy 2.0 + asyncpg + Pydantic   │
├──────────────────────────────────────────────────────────┤
│                    OLTP DATABASE                           │
│  PostgreSQL 16 + pgvector + JSONB + Partitioning          │
├──────────────────────────────────────────────────────────┤
│                    ORCHESTRATION                            │
│           Apache Airflow 2.x (DAGs + BashOperator)         │
├──────────────────────────────────────────────────────────┤
│              DISTRIBUTED PROCESSING                        │
│         Apache Spark 3.x + Structured Streaming            │
├──────────────────────────────────────────────────────────┤
│              ANALYTICAL STORAGE                             │
│     Delta Lake + Hive Metastore (catalog) + MinIO (S3)     │
├──────────────────────────────────────────────────────────┤
│                QUERY FEDERATION                              │
│                     Apache Trino                            │
└──────────────────────────────────────────────────────────┘
```

---

## PART II — CYCLE 1 SCHEDULE

### Cycle Overview

| # | Track | Type | Est. Length | Topic |
|---|--------|------|--------------|------|
| 01 | DT | Presentation | 10–12 min | Channel + NexoMart Case + Motivations |
| 02 | DT | Fundamentals | 10–12 min | Data Model + Choosing PostgreSQL |
| 03 | FT | Hands-on | 18–22 min | Relational Modeling — Tables, Constraints, Relationships |
| 04 | FT | Hands-on | 18–22 min | Normalization, SCD Type 2 and pgvector |
| 05 | FT | Hands-on | 18–22 min | JSONB — Semi-Structured Without Giving Up ACID |
| 06 | FT | Hands-on | 18–22 min | Indexing JSONB — GIN, Expression Indexes, and EXPLAIN ANALYZE |
| 07 | FT | Hands-on | 18–22 min | Advanced PostgreSQL — Partitioning, Window Functions, and CTEs |
| 08 | FT | Hands-on | 15–20 min | pgvector and Full Text Search — PostgreSQL as an AI Database |

---

### Video 01 — DT: Channel and NexoMart Case Presentation

**Goal**: Introduce who you are, what problem the channel solves, and plant the hook for the next video.

**Script structure:**

```
00:00 - 00:30  Hook — "This pipeline processes 50M records. All open-source,
                        all on your own machine. Let's build it from scratch?"
00:30 - 01:30  Who I am and why this channel exists
01:30 - 03:30  The problem: OLTP isn't OLAP (a pain the viewer recognizes)
03:30 - 06:00  Introducing NexoMart — the case we're building together
06:00 - 08:30  The 7 business domains and what happens in each one
08:30 - 10:30  The full stack — why each tool was chosen
10:30 - 12:00  What you'll learn throughout the series + CTA
```

**Required points:**
- Show the `data_platform_architecture.png` diagram (stack overview)
- Show `business_db.png` (NexoMart's OLTP schema)
- Briefly demo the terminal with Docker Compose spinning up to prove it works
- Channel positioning line: *"This channel is for data engineers who want to build a modern, open-source, production-like platform from scratch, without depending on proprietary cloud."*

**Hook for Video 02:**
> *"But before spinning up any container, there's a decision that will determine whether all of this works — choosing the database and how the data is modeled. In the next video, I'll show you why I chose PostgreSQL over MongoDB, Cassandra, or DynamoDB — and why that choice matters more than you'd think."*

---

### Video 02 — DT: Data Model and Database Choice

**Goal**: Technically justify why PostgreSQL is the right choice for NexoMart's OLTP and introduce the data model.

**Script structure:**

```
00:00 - 00:30  Recap of the previous video (15 sec)
00:30 - 02:00  The problem of choosing a database (it's not about trends)
02:00 - 05:00  Comparison: PostgreSQL vs alternatives
                - MongoDB: flexibility vs consistency (ACID)
                - Cassandra: scale vs operational complexity
                - DynamoDB: managed vs vendor lock-in and cost
05:00 - 08:00  Why PostgreSQL wins for a marketplace OLTP
                - Native ACID
                - JSONB (best of both worlds)
                - pgvector (AI-ready without switching databases)
                - Native partitioning
                - Extensions ecosystem
08:00 - 11:00  NexoMart's data model (business_db.png walkthrough)
                - The 7 schemas and their roles
                - Relationships between domains
                - Conventions: snake_case, AuditMixin, schema-qualified tables
11:00 - 12:00  Hook for the next video
```

**Required points:**
- Visual comparison table (PostgreSQL vs MongoDB vs Cassandra)
- Show `business_db.sql` — the full DDL
- Explain `AuditMixin` (created_at, updated_at, is_active, row_guid)
- Mention that pgvector shows up in the product videos further ahead

**Hook for Video 03:**
> *"Now that you know why we chose PostgreSQL, in the next video we go deep into the modeling — row by row, constraint by constraint. Why does this table have JSONB? Why does this relationship use UUID instead of BIGSERIAL? The decisions that look small here will make all the difference once volume scales up."*

---

### Video 03 — FT: Relational Modeling — Tables, Constraints, and Relationships

**Goal**: Hands-on walkthrough of the data model — focused on design decisions (not just DDL).

**Script structure:**

```
00:00 - 00:30  Hook + Recap
00:30 - 02:00  What we'll cover today (visual schema map)
02:00 - 06:00  product schema — ProductModel, ProductCategory, ProductModel
                - Why UUID as PK (distributed, no global sequence)
                - GIN indexes on the attributes JSONB
                - The vector(1536) field — a pgvector spoiler
06:00 - 10:00  person schema — Customer, Address, Contact
                - FK with ON DELETE CASCADE vs SET NULL (trade-offs)
                - Normalization: why address is a separate table
                - CPF/CNPJ validation: constraint vs application logic
10:00 - 15:00  sales schema — SalesOrder, SalesOrderItem (JSONB)
                - Why order_items is JSONB and not a relational table
                - Transactional ID prefix: SO-XXXXXXXXX
                - Status with a typed Enum (Python + PostgreSQL)
15:00 - 18:00  warehouse schema — Inventory, TransactionHistory
                - Partitioning by date (preview)
                - TR-XXXXXXXXX as the prefix
18:00 - 20:00  Demo: creating the tables in the local database + checking with \d
20:00 - 22:00  Recap + hook
```

**Required points:**
- Show `base.py` with the full `AuditMixin`
- Explain the difference between `UUID` and `BIGSERIAL` in distributed systems
- Demo the actual table creation via `uv run python` or Alembic

**Hook for Video 04:**
> *"But there are two topics I deliberately left for tomorrow — SCD Type 2, which is how NexoMart's HR keeps the complete history of every employee without losing anything, and pgvector, the extension that turns PostgreSQL into an AI database. If you work with data and still don't know what SCD Type 2 is, the next video is mandatory."*

---

### Video 04 — FT: Normalization, SCD Type 2, and pgvector

**Goal**: Cover the model's two advanced topics — history tracking and AI-readiness.

**Script structure:**

```
00:00 - 00:30  Recap
00:30 - 03:00  The history problem: tables that only keep the present
                - Example: an employee changes role. What happens to the old record?
03:00 - 08:00  SCD Type 2 in humanresources
                - valid_from / valid_to / is_current
                - How the INSERT + UPDATE works
                - Query to get the state at any point in time
                - Comparison: SCD1 vs SCD2 vs SCD3 vs SCD4
08:00 - 12:00  pgvector — PostgreSQL as a vector database
                - What an embedding is (a coordinates analogy)
                - Installing and enabling the extension
                - The `vector(1536)` field in ProductModel
                - Operators: <-> (L2), <#> (inner product), <=> (cosine)
12:00 - 17:00  Demo: generating embeddings with OpenAI/a local model and inserting into PG
                - Similarity query: "products similar to this one"
                - Creating an HNSW index for search at scale
17:00 - 20:00  Why this matters for the platform's future
                - Product recommendations
                - Semantic search over the catalog
                - RAG over NexoMart's data
20:00 - 22:00  Recap + hook toward dimensional modeling
```

**Hook for Video 05 (JSONB series):**
> *"In the next videos we go deep into a specific PostgreSQL feature that I think is the most underrated in the data community: JSONB. It's not just a JSON column — it's a native, indexable type that completely changes how you design certain tables. Come with me."*

---

### Video 05 — FT: JSONB — Semi-Structured Without Giving Up ACID

**Goal**: Present JSONB as a design tool, not just a "JSON column."

**Why 3+ PostgreSQL videos?**
PostgreSQL has features that justify dedicated coverage because they're real differentiators for data engineers. Many professionals know basic SQL but never explore the database's native analytical potential — these videos position the channel as a technical reference for depth.

**Script structure:**

```
00:00 - 00:30  Hook: "This query filters inside a JSON field with an index.
                       Response time: 12ms. 2 million records. No ORM."
00:30 - 02:00  json vs jsonb — why jsonb is always the choice
                - json: text, preserves formatting, not efficiently indexable
                - jsonb: binary, key deduplication, indexable with GIN
02:00 - 06:00  When to use JSONB (and when NOT to)
                - Yes: attributes that vary by product category
                - Yes: order items (avoiding unnecessary 1:N for reads)
                - Yes: event metadata (audit logs, telemetry)
                - No: fields you filter on frequently in critical queries
                - No: fields that need FK constraints
                - No: when the structure is 100% known and stable
06:00 - 10:00  Core operators (with live demo)
                - -> and ->> (access by key/index)
                - #> and #>> (access by path)
                - @> and <@ (containment)
                - ? and ?| and ?& (key existence)
                - jsonb_each() and jsonb_array_elements()
                - jsonb_build_object() and jsonb_agg()
10:00 - 15:00  JSONB in NexoMart: order_items
                - The column's real structure in SalesOrderItem
                - Inserting orders with JSONB
                - Querying: "total items from a category in the order"
                - Expanding JSONB into rows with jsonb_array_elements
15:00 - 18:00  JSONB in NexoMart: product attributes
                - Attributes that vary by category (clothing: size/color, electronics: voltage/weight)
                - Filtering by attribute: WHERE attributes @> '{"color": "blue"}'
18:00 - 20:00  Recap + hook toward JSONB indexes
```

**Hook for Video 06:**
> *"The problem is that without the right index, that query will run a full table scan across 2 million rows every single time. In the next video we're going to fix that with a GIN index — and I'll show you in EXPLAIN ANALYZE the difference between a query that takes 4 seconds and one that takes 12 milliseconds."*

---

### Video 06 — FT: Indexing JSONB — GIN, Expression Indexes, and EXPLAIN ANALYZE

**Goal**: Show the real impact of correct JSONB indexing with visual evidence via EXPLAIN ANALYZE.

**Script structure:**

```
00:00 - 00:30  Hook: EXPLAIN ANALYZE showing Seq Scan vs Bitmap Index Scan
00:30 - 02:00  JSONB recap + why indexing is critical
02:00 - 06:00  GIN Index — how it works internally
                - Inverted index: maps values → rows
                - jsonb_ops vs jsonb_path_ops
                  - jsonb_ops: supports ?, ?|, ?&, @>, @?
                  - jsonb_path_ops: only @>, more compact and faster for containment
                - Write cost vs read gain
06:00 - 10:00  Demo: creating and comparing GIN indexes
                - CREATE INDEX gin_order_items ON sales.sales_order USING GIN (order_items);
                - EXPLAIN ANALYZE before and after
                - Measuring: planning time, execution time, heap fetches
10:00 - 14:00  Expression Indexes on JSONB
                - When to use: a specific field accessed frequently
                - CREATE INDEX ON product.product ((attributes->>'category'));
                - Partial indexes: WHERE attributes->>'status' = 'active'
                - Size comparison: full GIN vs expression index
14:00 - 18:00  BRIN Index for temporal columns
                - Ideal for tables partitioned by date (sales, warehouse)
                - How it works: min/max per physical block
                - Minimal storage cost, excellent for time-series
18:00 - 20:00  Decision guide: which index to use?
                | Use case                        | Recommended index    |
                |---                              |---                    |
                | JSONB containment (@>)          | GIN jsonb_path_ops   |
                | JSONB key existence (?)         | GIN jsonb_ops        |
                | Specific JSONB field            | Expression Index     |
                | Timestamp column in a fact table| BRIN                 |
                | Text with like %term%           | GIN + pg_trgm        |
                | High-cardinality column         | B-Tree (default)     |
20:00 - 22:00  Recap + hook
```

**Hook for Video 07:**
> *"Now that you know how to index JSONB, let's move to the two PostgreSQL features data analysts need most but know least: Window Functions and table Partitioning. Window Functions will replace subqueries you're writing today — and they're up to 10x faster. Let's go."*

---

### Video 07 — FT: Advanced PostgreSQL — Partitioning, Window Functions, and CTEs

**Goal**: Cover PostgreSQL's native analytical features that bring OLTP closer to OLAP.

**Script structure:**

```
00:00 - 00:30  Hook
00:30 - 02:00  Context: PG as a hybrid OLTP + light-OLAP database
02:00 - 07:00  Table Partitioning
                - Range Partitioning: sales_order by order_date (per month/year)
                - List Partitioning: warehouse by region/hub
                - Partition Pruning: how the planner skips irrelevant partitions
                - Demo: EXPLAIN for a query with and without pruning
                - Constraint Exclusion + declarative partitioning (PG 10+)
07:00 - 13:00  Window Functions — Analytical SQL Without Subqueries
                - Syntax: OVER (PARTITION BY ... ORDER BY ...)
                - ROW_NUMBER() — ranking orders per customer
                - RANK() and DENSE_RANK() — top products per category
                - LAG() and LEAD() — month-over-month revenue variation
                - SUM() OVER — running total revenue
                - NTILE() — segmenting customers into quartiles (RFM preview)
                - Live demo with NexoMart data
13:00 - 17:00  CTEs — Common Table Expressions
                - WITH clause: readability vs performance
                - Recursive CTEs: product category hierarchy
                - Materialized vs non-materialized (PG 12+)
                - When a CTE beats a subquery (and when it doesn't)
17:00 - 20:00  Materialized Views
                - Use case: pre-computed daily revenue aggregation
                - REFRESH MATERIALIZED VIEW CONCURRENTLY
                - Limitation: not incremental (preview: this is what dbt solves)
20:00 - 22:00  Recap + double hook
```

**Hook for Video 08 (pgvector) and for Dimensional Modeling:**
> *"In the next video we close out the PostgreSQL cycle with pgvector and Full Text Search — real AI inside the database. And after that, we move into dimensional modeling: Star Schema, facts, dimensions, grain — where the data stops being operational and becomes truly analytical. That's the heart of every data platform."*

---

### Video 08 — FT: pgvector and Full Text Search — PostgreSQL as an AI Database

**Goal**: Show that PostgreSQL can serve as both a vector database and a semantic search engine without adding infrastructure.

**Script structure:**

```
00:00 - 00:30  Hook: "Semantic search on products with no Elasticsearch, no Pinecone,
                       no extra service at all. Just PostgreSQL."
00:30 - 03:00  The problem: keyword search vs meaning search
                - "running shoes" doesn't return "athletic footwear for track"
                - Where Full Text Search solves it and where you need vectors
03:00 - 08:00  Native Full Text Search
                - tsvector and tsquery
                - to_tsvector('english', name || ' ' || description)
                - GIN index on tsvector
                - ts_rank() for relevance ranking
                - Dictionary configuration
                - Demo: term search over the NexoMart catalog
08:00 - 12:00  pg_trgm — fuzzy search for typos
                - Trigram similarity (%)
                - GIN index with pg_trgm
                - Use case: "product suggestion" as you type
12:00 - 18:00  pgvector — embeddings in the database
                - What an embedding is (coordinates in semantic space)
                - Installing: CREATE EXTENSION vector
                - The vector(1536) field in ProductModel
                - Generating embeddings: OpenAI API or a local model (nomic-embed)
                - Inserting embeddings: UPDATE product SET embedding = '[0.1, 0.2, ...]'
                - Distance operators: <-> L2, <#> inner product, <=> cosine
                - IVFFlat vs HNSW index (trade-off: build time vs query time)
                - Demo: "products similar to this one" with cosine similarity
18:00 - 20:00  When to use each approach
                | Use case                          | Solution               |
                |---                                |---                    |
                | Exact keyword search               | ILIKE / B-Tree       |
                | Search with term variations         | Full Text Search     |
                | Typo tolerance                      | pg_trgm              |
                | Search by meaning/context            | pgvector             |
                | All of it combined                  | Hybrid search        |
20:00 - 22:00  Hook toward Cycle 2 (API Modeling) + Dimensional
```

**Hook for Cycle 2:**
> *"Now that the database is modeled, indexed, and AI-ready, the next cycle starts with the API — the layer that feeds all of this. Asynchronous FastAPI, SQLAlchemy 2.0, 3-layer architecture, and how to generate volume data with chaos engineering to simulate a real environment. See you there."*

---

## PART III — DEVELOPMENT STAGES

### Phase 1: Foundation (parallel to the Cycle 1 video series)

**Goal**: Have the functional base ready to demo in videos 01–04.

| Task | Artifacts | Status |
|---|---|---|
| Complete OLTP schema | `business_db.sql`, `business_db.png` | Done |
| Architecture diagram | `data_platform_architecture.png` | Done |
| SQLAlchemy models (7 domains) | `dbapi/api/models/` | Done |
| AuditMixin and Base | `dbapi/api/config/base.py` | Done |
| Docker Compose configuration (all services) | `docker-compose.yml` per service | Done |
| Seeders with chaos engineering | `dbapi/api/seeder/` | Done |
| 3-layer architecture (Repository/Service/Router) | `dbapi/api/` | Done |

### Phase 2: PostgreSQL Features (for videos 05–08)

**Goal**: Build reproducible demos for the PG feature videos.

| Task | Artifacts | Priority |
|---|---|---|
| pgvector setup script | `demos/pgvector_setup.sql` | High |
| Embeddings notebook with a local model | `demos/embeddings_demo.ipynb` | High |
| JSONB scripts with EXPLAIN ANALYZE | `demos/jsonb_benchmarks.sql` | High |
| Partitioning script (sales by month) | `demos/partitioning_demo.sql` | High |
| Window Functions scripts with real data | `demos/window_functions.sql` | High |
| Full Text Search script | `demos/fts_demo.sql` | Medium |
| Comparative GIN index script | `demos/gin_index_benchmark.sql` | High |
| Dedicated demo-volume seeder (100k records) | `demos/demo_seeder.py` | High |

### Phase 3: Dimensional Modeling (Cycle 2 — next)

**Goal**: Build the dimensional Data Warehouse on top of NexoMart's OLTP.

| Task | Description |
|---|---|
| NexoMart Bus Matrix | 5 facts × 9 dimensions mapped |
| Star Schema diagram | Visual for use in Cycle 2's EP01 |
| Fact tables | fact_sales, fact_inventory, fact_marketing_performance |
| Dimensions | dim_product, dim_customer, dim_date, dim_location, dim_seller |
| Slowly Changing Dimensions | dim_employee with SCD2 (inherited from OLTP) |
| Grain declaration | 1 row per order_item (most granular possible) |

---

## PART IV — POSTGRESQL FEATURES: COVERAGE GUIDE

### Full feature-to-video map

| Feature | Video | Depth |
|---|---|---|
| JSONB vs JSON | 05 | Conceptual + hands-on |
| JSONB operators (->, ->>, @>, ?) | 05 | Hands-on |
| JSONB functions (jsonb_each, jsonb_agg) | 05 | Hands-on |
| GIN Index (jsonb_ops vs jsonb_path_ops) | 06 | Deep |
| Expression Index on JSONB | 06 | Deep |
| BRIN Index (time-series) | 06 | Conceptual |
| EXPLAIN ANALYZE | 06 | Deep |
| Partitioning (Range, List) | 07 | Deep |
| Partition Pruning | 07 | Demo |
| Window Functions (ROW_NUMBER, LAG, SUM OVER) | 07 | Deep |
| CTEs and Recursive CTEs | 07 | Deep |
| Materialized Views | 07 | Conceptual |
| Full Text Search (tsvector, tsquery, GIN) | 08 | Deep |
| pg_trgm (fuzzy search) | 08 | Hands-on |
| pgvector (embeddings, IVFFlat, HNSW) | 04 + 08 | Deep |
| SCD Type 2 (valid_from, valid_to) | 04 | Deep |
| Enum types (Python + PostgreSQL) | 03 | Conceptual |
| UUID vs BIGSERIAL | 03 | Conceptual |
| CHECK and UNIQUE constraints | 03 | Hands-on |
| Schema-qualified tables | 03 | Conceptual |

### Features reserved for future cycles

| Feature | Suggested cycle | Reason |
|---|---|---|
| LISTEN / NOTIFY | Cycle 3 (Kafka) | Comparison with messaging |
| Row Level Security | Cycle 4 (Auth) | Contextualized with multi-tenant |
| pg_partman (partition automation) | Cycle 3 | After basic partitioning |
| Logical Replication / CDC | Cycle 3 (Debezium) | Alongside the Outbox Pattern |
| pg_cron | Cycle 2 (Airflow) | Comparison with orchestration |
| Vacuuming and bloat management | Future cycle (Ops) | DBA track |

---

## PART V — HOOKS BETWEEN CYCLES

### Cycle 1 → Cycle 2 (API Modeling)

Cycle 1 ends with the database modeled and documented. The natural hook into Cycle 2 is:

> *"The database is ready. Now the question is: how do you insert 50 million records without bringing everything to a halt? In Cycle 2, we build the ingestion layer: asynchronous FastAPI, SQLAlchemy 2.0, 3-layer architecture, and seeders with chaos engineering."*

### Cycle 2 → Cycle 3 (Dimensional Modeling)

> *"The data is in the OLTP database. But OLTP wasn't built for analytics. Now we're going to build the dimensional model — Star Schema, facts, dimensions, and the first ETL pipeline with Spark and Delta Lake."*

---

*Document created 2026-04-28 | Project: Data Platform From Scratch — NexoMart*
