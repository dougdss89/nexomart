# 🚀 Content Creation Roadmap: Data Engineering Portfolio API
This document maps out a structured series of ideas for **Medium** articles and **YouTube** videos, based entirely on the real solutions and engineering challenges we solved in the `data_platform/dbapi` project.

The order below was designed to create a "Series" or "Track", where each piece of content connects to and sets up the next, without repetition.

---

## 🏗️ Phase 1: Foundations and Architecture

### 1. The End of Perfect CSVs: How to Build a Realistic Data Generator for Data Engineering Portfolios
* **The Problem:** Most portfolio projects use clean Kaggle datasets, which don't reflect the toxic reality of corporate data (legacy ERPs/CRMs).
* **The Solution:** Present the architecture of your `dbapi`. How you split it into layers (`schemas`, `models`, `samples`, `seeders`, and `services`).
* **Highlight:** The difference between using a simple `Faker` in a loop _versus_ building a real combinatorial "factory" of corporate data organized into Postgres schemas (`person`, `product`, `sales`, `humanresources`).

### 2. Local Infrastructure for Data Pipelines: Why I Swapped the DockerOperator for HTTP Calls in Airflow
* **The Problem:** The hidden IOPS bottleneck on external HDDs and local machines when running Airflow.
* **The Math:** Explain how spinning up and tearing down Python containers for every task takes time and wrecks the disk.
* **The Solution:** Show the architectural decision to keep `dbapi` (FastAPI) hot in RAM (DDR5) and use Airflow only as a "conductor" triggering `SimpleHttpOperator`. A pure Microservices concept applied to Data Engineering.

---

## 🧬 Phase 2: Controlled Chaos (Data Quality & Engineering)

### 3. Chaos Engineering: How to Intentionally Inject Data Anomalies to Train ELT Pipelines
* **Focus:** The `api/samples/customer.py` file.
* **The Approach:** Show that a good engineer doesn't just clean data — they know how to predict the mess.
* **Code Showcase:**
  * Negative values in billing (chargeback bugs).
  * Inconsistency between masked and clean CPFs/CNPJs.
  * *Missing data* simulating frontend payload failures (e.g., arbitrary deletion of the `marketing_consent` node).

### 4. Schema Evolution in Practice: Handling Unpredictable "Nested JSON" in PostgreSQL
* **The Concept:** Relational databases hate column changes. How do you survive that?
* **The Implementation:** Using the `demographics` column as `JSONB` in the Customer table.
* **Highlight:** Show the injection of *Schema Drift* (5% of customers suddenly generating the `lifetime_value_score` key). Demonstrate how Pydantic with `extra="allow"` saves the application and (intentionally) passes the problem down to the Bronze layer of the Data Lake to handle.

---

## ⚙️ Phase 3: Performance, Algorithms, and Advanced Modeling

### 5. Escape the ORM Trap: How to Insert 200,000 Records in Seconds Using SQLAlchemy Core
* **The Problem:** Writing `session.add()` loops in SQLAlchemy is a performance disaster for batch loads.
* **The Solution:** Show the refactoring of the `services` (e.g., `product_service.py`), dropping object-oriented modeling when saving and using `insert().values()` with bulk dictionaries.
* **Vanity Metric:** Time comparison between saving 50,000 rows via ORM vs. Atomic Bulk Insert. Extremely relevant for heavy ingestion.

### 6. Dynamic Relationships in Mocks: Avoiding Hardcoded Foreign Keys
* **The Problem:** Seeder scripts tend to break when the source database updates its limited instances.
* **The Solution:** Show the refactor in `person_customer.py` where we read the exact size of the `PAISES_OPERACAO` and `ESTADOS` arrays.
* **Concept:** Resilient code. If the team adds 10 new countries tomorrow, the generator adapts mathematically without touching a single line of seeder logic.

### 7. Slowly Changing Dimensions (SCD Type 2) at the Source: Generating Functional History
* **The Concept:** What SCD Type 2 is and why it's the terror of junior engineers.
* **The Practice:** The genius behind `humanresources_job_role.py`.
* **Showcase:** Show the mathematical logic behind generating "plausible pasts". How to make Python calculate old effective dates correlated with salaries that undergo random deflation (-10% to -25%) to simulate real career progressions and raises instead of random numbers.

---

## 🤖 Phase 4: The Future (AI and Messaging)

### 8. Preparing Relational Data for AI with PGVector and PostgreSQL
* **The Concept:** The LLM hype demands vectorized data, and the Data Engineer is the one who needs to prepare that infrastructure.
* **The Implementation:** The `product_embedding.py` seeder.
* **Showcase:** Emulating the generation of 1536-dimension vectors simulating product text *embeddings*. The importance of tagging AI metadata columns (model used, token count) in the table.

### 9. Event-Driven Architecture: Preparing Tables for CDC (Change Data Capture)
* **The Concept:** Batch extractions are dying, streaming is the present.
* **The Forecast:** How our tables, through fields like `updated_at`, `row_guid`, and especially our `source_data_hash` trick (seen in product embeddings), leave fertile ground to plug in a **Debezium** or **Kafka** in the future and react only to modified data.

---

💡 **Strategic Tip for YouTube:**
For each video, make the source code snippets available as "Gists" on GitHub and put them in the description. At the end of the series, announce that the full API repository is open. This creates traction and massive engagement on your LinkedIn and GitHub.
