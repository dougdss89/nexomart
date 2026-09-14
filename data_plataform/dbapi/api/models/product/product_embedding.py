# models/product.py
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from api.config.base import Base, AuditMixin

class ProductEmbeddingModel(Base, AuditMixin):
    __tablename__ = "productembedding"
    __table_args__ = {"schema": "product"} # Organization by Schemas in Postgres

    embedding_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, unique=True, nullable=False) # Add ForeignKey if the product table already exists

    # --- VECTORS ---
    # The Vector type is imported from pgvector.sqlalchemy
    description_vector = Column(Vector(1536), nullable=True)
    image_vector = Column(Vector(512), nullable=True)

    # --- CACHE / ETL CONTROL ---
    source_data_hash = Column(String(64), nullable=False)

    # --- THE JSONB SECRET ---
    # Everything that was a sub-model in Pydantic (AIModelMetadata) simply becomes JSONB here
    ai_metadata = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    