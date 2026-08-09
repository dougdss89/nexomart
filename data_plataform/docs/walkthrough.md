# Product Services Implementation Walkthrough

## 1. Summary of Actions
To enable data creation for the schemas and models already defined, I implemented the Services layer (`services`) focused on the `product` schema. These services are responsible for receiving data (validated by the Pydantic schemas) and persisting it to the database via `SQLAlchemy AsyncSession`.

During planning, I found and fixed the following issues in the existing models to avoid import conflicts:
- **`ProductModel` vs `Product`**: Renamed the class in `product.py` to `Product` and the class in `productcategory.py` from `ProductCategorModel` to `ProductCategoryModel` (also fixing the typo).

## 2. Changes Made
The following files were created to make up the `dbapi` backend functions:

### Naming Fix
- [api/models/product/product.py](file:///home/doug/project/data_plataform/dbapi/api/models/product/product.py) (`ProductModel` -> `Product`)
- [api/models/product/productcategory.py](file:///home/doug/project/data_plataform/dbapi/api/models/product/productcategory.py) (`ProductCategorModel` -> `ProductCategoryModel`)

### New Services (Batch/Bulk Insert Pattern for CDC)
- **[api/services/product/product_category_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_category_service.py)**: `bulk_insert_product_categories` service
- **[api/services/product/product_model_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_model_service.py)**: `bulk_insert_product_models` service
- **[api/services/product/product_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_service.py)**: `bulk_insert_products` service
- **[api/services/product/product_embedding_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_embedding_service.py)**: `bulk_insert_product_embeddings` service

## 3. Dynamic Mock Data Generation (200k SKUs)
To reach the goal of populating the database with about 200,000 realistic and diverse products:
- **[api/samples/product.py](file:///home/doug/project/data_plataform/dbapi/api/samples/product.py)**: Refactored into an advanced combinatorial generator. I created per-category subclasses (such as `SmartphoneSamples`, `LaptopSamples`, `WomensApparelSamples`, and new ones like `MusicalInstrumentsSamples`, `GamingConsoleSamples`, etc.) equipped with base-name lists, brands, colors, sizes, and realistic price variations. The `generate_randomized_catalog(n)` function dynamically sweeps all factories and builds pseudo-real products that can exceed billions of unique combinations.
- **[api/seeder/product/product.py](file:///home/doug/project/data_plataform/dbapi/api/seeder/product/product.py)**: Updated to no longer use the local hardcoded apparel blocks, and instead consume `generate_randomized_catalog` directly. It cross-references the output with its Enum standard (mapping the safe FK) and injects everything into a database-JSONB-friendly layout (respecting UUIDs, insertion date, etc).

## 4. Data Engineering Challenges (SCD/CDC/Data Quality)
Since the goal of the project is to simulate a real, complex environment to be handled by data pipelines (ETL/ELT), I intentionally added "Dirty Data" (Data Anomalies) characteristics to product generation:
- **Flexible Schema Evolution**: Enabled `class Config(extra="allow")` in the detail Pydantic model (`ProductDetails`). The routine injects JSONB properties that don't always exist on every product (such as `is_refurbished`, `voltage`, or `warranty_months`). This forces the data engineer to handle dynamic columns (unpredictable structs).
- **Nulls and Values**: Injected `null` noise (e.g. ~4% chance that `depth` isn't mapped in the dimensions) and ~2% chance that `weight` comes back negative (`minus`), simulating human input error.
- **Formatting Errors (Typos)**: Created a probability (~5%) that `brand` strings come back in `UPPERCASE` and `color` in `lowercase`, to exercise standardization/cleanup routines during CDC or Pub/Sub consumption.
- **Zeroed Tax Fields**: Rates can arbitrarily come back as `0.0`.

## 5. AI Vector Mocks (Product Embedding Seeder)
Thinking ahead to the project's evolution toward semantic search, I created **[api/seeder/product/product_embedding.py](file:///home/doug/project/data_plataform/dbapi/api/seeder/product/product_embedding.py)**:
- It generates vectors compatible with `pgvector`, simulating outputs from large models (e.g. OpenAI Text-Embedding with 1536 dimensions and OpenAI CLIP-ViT for images with 512 dimensions).
- Injects the metadata log of the AI simulations, including failures (e.g. when image extraction fails and returns the array as `null`, creating further data-cleaning challenges).
- Generates the SHA256 hash of the original text extracted from the generator's combinations. This hash allows future simulation of caching-control pipelines (only reprocessing embeddings if the original text string sent via CDC differs from the hash stored in Postgres).

## 6. Engineering Best Practices (Services Layer)
Refactored all four `services` files to prepare the ground for scalable orchestration in Airflow, applying **Software/Data Engineering** principles:
1. **Bulk Insert (SQLAlchemy 2.0 Core)**: Replaced ORM-instance-based insertion (`db.add`) with SQLAlchemy Core (`insert(Model).values()`). When inserting **200,000 SKUs**, the overhead of instantiating the ORM object in memory chokes the node. With `insert().values(list_of_dicts)`, it performs one massive, atomic bulk insert, cutting ingestion from hours down to a few minutes or seconds.
2. **Transactional Robustness (Try/Except/Rollback)**: Added a `try/except SQLAlchemyError` block around the commit attempt. If any of the 200k SKUs violates a Foreign Key partway through, it catches the exception, immediately does `db.rollback()`, and raises an error log. Most importantly: the code re-raises the exception, informing the external tool (the Airflow Sensor/Operator) that the task failed, cleanly triggering the DAG's *retry* queue instead of the DAG assuming success while corrupting the database.
3. **Type Hinting and Early Returns (Guard Clauses)**: Applied signature typing (`List[Dict[str, Any]]`) for Mypy linting and for a senior developer to grasp at a glance. Also added `if not data: return False`, which avoids needlessly opening Postgres transactions if the insert list arrives empty at the Airflow worker.

These services can be conveniently imported from `api.services.product` thanks to the configured `__init__.py`.

## 6. How to verify (Test)
To validate the integrated operation of your database, SQLAlchemy, and Pydantic without needing to start the REST API, you can run the test script left at `/home/doug/project/data_plataform/dbapi/test_product_services.py`.

1. Access your Docker container (or an environment with access to your Postgres URL and dependencies installed):
   ```bash
   cd /home/doug/project/data_plataform/dbapi
   uv run python test_product_services.py
   ```
2. The script will handle the async communication, connecting and step-by-step verifying the virtual persistence of Category, Model, base Product, and their embeddings. (It performs a `rollback` at the end so it doesn't leave test mocks in your database.)

> Keep in mind this sets the stage perfectly for implementing the FastAPI routes in the future.
