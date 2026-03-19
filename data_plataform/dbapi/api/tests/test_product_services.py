import asyncio
from api.database.database import AsyncSessionLocal
from api.services.product import (
    create_product_category,
    create_product_model,
    create_product,
    create_product_embedding
)
from api.schemas.product.production_product_category import ProductCategorySchema, ProductCategory
from api.schemas.product.production_product_model import ProductModelSchema, ProductModel as ProductModelEnum
from api.schemas.product.production_product import ProductSchema, ProductDetails
from api.schemas.product.productembedding import ProductEmbeddingSchema, AIModelMetadata
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Starting verification of Product Services...")
    
    async with AsyncSessionLocal() as db:
        try:
            # 1. Test Create Product Category
            logger.info("Creating Product Category...")
            category_schema = ProductCategorySchema(
                category_id=1,
                category_name=ProductCategory.ELECTRONICS,
                created_date="2026-01-01T00:00:00Z"
            )
            category = await create_product_category(db, category_schema)
            logger.info(f"Category created with ID: {category.category_id}")

            # 2. Test Create Product Model
            logger.info("Creating Product Model...")
            model_schema = ProductModelSchema(
                product_model_id=1,
                model_name=ProductModelEnum.SMARTPHONE
            )
            p_model = await create_product_model(db, model_schema)
            logger.info(f"Model created with ID: {p_model.productmodel_id}")

            # 3. Test Create Product
            logger.info("Creating Product...")
            product_schema = ProductSchema(
                product_id=1,
                product_name="Super Phone X",
                model_id=p_model.productmodel_id,
                category_id=category.category_id,
                product_details=ProductDetails(
                    list_price=100.0,
                    price=299.0,
                    tax=29.0,
                    brand="TechBrand",
                    sku="SPX-001"
                )
            )
            product = await create_product(db, product_schema)
            logger.info(f"Product created with ID: {product.product_id}")

            # 4. Test Create Product Embedding
            logger.info("Creating Product Embedding...")
            embedding_schema = ProductEmbeddingSchema(
                embedding_id=1,
                product_id=product.product_id,
                description_vector=[0.1] * 1536,
                image_vector=[0.2] * 512,
                source_data_hash="abc123hash",
                ai_metadata=AIModelMetadata(
                    text_model="openai-text-embedding-3-small",
                    image_model="openai-clip-vit-base-patch32"
                )
            )
            embedding = await create_product_embedding(db, embedding_schema)
            logger.info(f"Embedding created with ID: {embedding.embedding_id}")

            logger.info("All services executed successfully. Rolling back to avoid dirtying the database.")
            await db.rollback()

        except Exception as e:
            logger.error(f"Error during verification: {e}")
            await db.rollback()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
