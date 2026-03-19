# Task Checklist

## Planning
- [x] Analyze existing SQLAlchemy models for `product`
- [x] Analyze existing Pydantic schemas for `product`
- [x] Determine how the database session is managed
- [x] Create implementation plan for the services layer
- [x] Request user review for the plan

## Implementation
- [x] Create services for Product Category
- [x] Create services for Product Model
- [x] Create services for Product
- [x] Create services for Product Embedding

## Verification
- [x] Verify if the services can be correctly instantiated and use the given session

## Dynamic Product Seeder
- [x] Refactor `api/samples/product.py` to act as a combinatorial generator
- [x] Update `api/seeder/product/product.py` to use the new generator and support 200k SKUs

## Data Engineering Challenges Simulation
- [x] Expand `ProductCategory` and `ProductModel` Enums with more variations
- [x] Allow flexible/dynamic attributes in `ProductDetails` Pydantic schema
- [x] Create specialized Sample Generators for new categories
- [x] Introduce intentional Data Quality anomalies (nulls, typos, negative values) in the mock generator

## Product Embedding Seeder
- [x] Review `product_embedding` schema and database model
- [x] Create `api/seeder/product/product_embedding.py` com mock vector generation
