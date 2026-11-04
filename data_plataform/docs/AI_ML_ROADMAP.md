# AI/ML — Use-Case Roadmap (NexoMart)

> **Status**: Planning only. Nothing here has been implemented yet — brainstorm session on 2026-07-12, with the explicit goal of "laying the groundwork" before building any piece. Revisit this doc when it's time to start.

## Objective

Show AI usage in the marketplace **beyond possibilities and theory** — i.e., use cases that produce a concrete table/decision within the data platform itself (Bronze → Silver → Gold), not just an isolated notebook.

## Hardware constraint

Local GPU: **RTX 4060 Laptop, 8GB VRAM** (verified via `nvidia-smi` — not a 4090 with 6GB, that combination doesn't exist). Docker GPU passthrough works via WSL2 (`nvidia` runtime confirmed). This caps local models (via Ollama, if/when it comes in) to around 7B at Q4 (~4-5GB), no more than that.

## What already exists as a foundation (no need to recreate)

- `dbapi/api/models/product/product_embedding.py`: `product.productembedding` table already modeled with `description_vector Vector(1536)` and `image_vector Vector(512)` (via `pgvector.sqlalchemy`), plus complete repository/service/schema/seeder (`api/seeder/product/product_embedding.py`, mock vector generation).
- **Pending**: the Postgres `vector` extension **is not enabled** yet in any database in the `pgprojeto` container (only `plpgsql` present, verified on 2026-07-11). Needs `CREATE EXTENSION IF NOT EXISTS vector;` before any real embedding will work.

## Agreed prioritization

### 1. Demand / inventory forecasting (start here)

Doesn't depend on an LLM/GPU — it's classic ML, a tangible result, a good first step.

- **Target**: `demand(product, store, day)`, from `fact_sales`.
- **Grain**: daily per product x store (or weekly if the series is too sparse per product).
- **Features**: lags (t-1, t-7, t-14, t-28), moving averages (7d/28d), day of week, month, holiday, promotion flag (`dim_promotion`), price (`dim_product`).
- **Sources**: `fact_sales` + `dim_date` + `dim_product` + `dim_promotion` + `dim_store` → new Gold table `gold_fact_sales_daily` (aggregation).
- **Model**: **global** LightGBM/XGBoost (a single model trained across all product x store series together — the pattern that wins in retail-forecasting competitions like M5, because it shares learning across similar products). Simpler alternative to start with: Prophet or statsforecast (AutoARIMA/ETS) per series, easier to explain but without that shared learning.
- **Where it runs**: Spark handles the heavy aggregation (Bronze/Silver → `gold_fact_sales_daily`); model training runs in pandas/single-node (once aggregated by day, the volume is no longer "big data" — an architecture decision worth stating explicitly: Spark for ETL, not for the whole ML pipeline).
- **Output**: new Gold table `gold_demand_forecast` (product, store, date, predicted_qty, confidence_interval) — queryable via Trino/dbt like any other Gold table. This is the strong point for the portfolio: the AI forecast becomes a first-class table in the data platform again.
- **Orchestration**: new Airflow DAG, same pattern as `dimbuyer_dag.py`/`iceberg_dag.py` — a weekly retrain DAG, a daily inference DAG.
- **Evaluation**: **temporal** split (never random — it leaks future information). WAPE/MAPE metric per product category, compared against a naive baseline (repeat last week's demand).

**Natural extension** (once demand is forecast): **purchase need / reorder point** — combine `predicted_qty` with `fact_purchase` (supplier lead time) to turn it into "buy X units of product Y by date Z." This is the piece that turns the forecast into an actionable business decision.

### 2. Semantic search / RAG over the product catalog

- Enable `CREATE EXTENSION vector` in Postgres, generate real embeddings (currently only mocked) for `product.productembedding.description_vector`.
- Local embedding model via Ollama (e.g. `nomic-embed-text`, 768 dim — note: would require migrating the column, currently at 1536) or an external API.
- Natural-language search over the catalog ("red bike for trails") via `<=>` (cosine distance) in pgvector.
- Possible extension: **text-to-SQL over Gold** (Delta/Trino) — user asks in Portuguese, a small model generates SQL against `nexomart` (Gold). Uses the whole stack (Spark, Delta, Trino) — good for an architectural demo.

### 3. Secondary ideas (backlog, no detailed design yet)

- **Product recommendations**: ALS (collaborative filtering) via native Spark MLlib, on top of `fact_sales` — doesn't depend on an LLM.
- **Customer segmentation / churn**: RFM + clustering (KMeans) and churn classification over `dim_buyer`/`fact_sales`.
- **Purchase anomaly detection**: outliers in `fact_purchase` (possible pricing error or fraud).

## Next step

None yet — come back here once the foundations are ready to decide which piece to implement first (current recommendation: #1, demand forecasting).
