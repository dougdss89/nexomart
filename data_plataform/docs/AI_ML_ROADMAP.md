# AI/ML — Roadmap de Casos de Uso (NexoMart)

> **Status**: Planejamento apenas. Nada aqui foi implementado ainda — sessão de brainstorm em 2026-07-12, com o objetivo explícito de "plantar as bases" antes de construir qualquer peça. Revisitar este doc quando for hora de começar.

## Objetivo

Mostrar uso de IA no marketplace **além de possibilidades e teoria** — ou seja, casos de uso que gerem uma tabela/decisão concreta dentro do próprio data platform (Bronze → Silver → Gold), não só um notebook isolado.

## Restrição de hardware

GPU local: **RTX 4060 Laptop, 8GB VRAM** (verificado via `nvidia-smi` — não é uma 4090 de 6GB, essa combinação não existe). Docker GPU passthrough funciona via WSL2 (`nvidia` runtime confirmado). Isso limita modelos locais (via Ollama, se/quando entrar) a algo em torno de 7B em Q4 (~4-5GB), não mais que isso.

## O que já existe como base (não é preciso recriar)

- `dbapi/api/models/product/product_embedding.py`: tabela `product.productembedding` já modelada com `description_vector Vector(1536)` e `image_vector Vector(512)` (via `pgvector.sqlalchemy`), mais repository/service/schema/seeder completos (`api/seeder/product/product_embedding.py`, geração mock de vetores).
- **Pendente**: a extensão `vector` do Postgres **não está habilitada** ainda em nenhum banco do container `pgprojeto` (só `plpgsql` presente, verificado em 2026-07-11). Precisa de `CREATE EXTENSION IF NOT EXISTS vector;` antes de qualquer embedding real funcionar.

## Priorização acordada

### 1. Previsão de demanda / estoque (começar por aqui)

Não depende de LLM/GPU — é ML clássico, resultado tangível, bom primeiro passo.

- **Target**: `demanda(produto, loja, dia)`, a partir de `fact_sales`.
- **Grão**: diário por produto x loja (ou semanal se a série for muito esparsa por produto).
- **Features**: lags (t-1, t-7, t-14, t-28), médias móveis (7d/28d), dia da semana, mês, feriado, flag de promoção (`dim_promotion`), preço (`dim_product`).
- **Fontes**: `fact_sales` + `dim_date` + `dim_product` + `dim_promotion` + `dim_store` → nova tabela Gold `gold_fact_sales_daily` (agregação).
- **Modelo**: LightGBM/XGBoost **global** (um modelo só, treinado em todas as séries produto x loja juntas — é o padrão que vence em competições de retail forecasting tipo M5, porque compartilha aprendizado entre produtos parecidos). Alternativa mais simples pra começar: Prophet ou statsforecast (AutoARIMA/ETS) por série, mais fácil de explicar mas sem esse compartilhamento.
- **Onde roda**: Spark faz a agregação pesada (Bronze/Silver → `gold_fact_sales_daily`); o treino do modelo roda em pandas/single-node (depois de agregado por dia o volume já não é mais "big data" — decisão de arquitetura a deixar explícita: Spark para ETL, não para o pipeline de ML inteiro).
- **Saída**: nova tabela Gold `gold_demand_forecast` (produto, loja, data, qtd_prevista, intervalo_confiança) — consultável via Trino/dbt como qualquer outra tabela Gold. Esse é o ponto forte pro portfólio: a previsão da IA volta a ser uma tabela de primeira classe no data platform.
- **Orquestração**: DAG novo no Airflow, mesmo padrão de `dimbuyer_dag.py`/`iceberg_dag.py` — um DAG semanal de retrain, um diário de inferência.
- **Avaliação**: split **temporal** (nunca aleatório — vaza informação de futuro). Métrica WAPE/MAPE por categoria de produto, comparado contra baseline ingênuo (repetir a demanda da semana anterior).

**Extensão natural** (depois que a demanda estiver prevista): **necessidade de compra / reorder point** — combinar `qtd_prevista` com `fact_purchase` (lead time do fornecedor) pra virar "compre X unidades do produto Y até a data Z". É essa peça que transforma a previsão em decisão acionável de negócio.

### 2. Busca semântica / RAG sobre o catálogo de produtos

- Habilitar `CREATE EXTENSION vector` no Postgres, gerar embeddings reais (hoje só tem mock) para `product.productembedding.description_vector`.
- Modelo de embedding local via Ollama (ex. `nomic-embed-text`, 768 dim — nota: exigiria migrar a coluna, hoje está em 1536) ou API externa.
- Busca em linguagem natural sobre o catálogo ("bicicleta vermelha pra trilha") via `<=>` (cosine distance) no pgvector.
- Extensão possível: **text-to-SQL sobre o Gold** (Iceberg/Trino) — usuário pergunta em português, modelo pequeno gera SQL contra `iceberglake.gold`/`nexo`. Usa a stack inteira (Spark, Iceberg, Trino, UC) — bom para demo arquitetural.

### 3. Ideias secundárias (backlog, sem desenho detalhado ainda)

- **Recomendação de produtos**: ALS (filtragem colaborativa) via Spark MLlib nativo, em cima de `fact_sales` — não depende de LLM.
- **Segmentação de clientes / churn**: RFM + clustering (KMeans) e classificação de churn sobre `dim_buyer`/`fact_sales`.
- **Detecção de anomalia em compras**: outliers em `fact_purchase` (possível erro de preço/fraude).

## Próximo passo

Nenhum ainda — voltar aqui quando as bases estiverem prontas para decidir qual peça implementar primeiro (recomendação atual: #1, previsão de demanda).
