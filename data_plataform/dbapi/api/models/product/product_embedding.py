# models/product.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from pgvector.sqlalchemy import Vector
from sqlalchemy.sql import func
from .base import Base
import uuid

class ProductEmbeddingModel(Base):
    __tablename__ = "productembedding"
    __table_args__ = {"schema": "product"} # Organização por Schemas no Postgres

    embedding_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, unique=True, nullable=False) # Adicione ForeignKey se a tabela product já existir
    
    # --- VETORES ---
    # O tipo Vector é importado do pgvector.sqlalchemy
    description_vector = Column(Vector(1536), nullable=True)
    image_vector = Column(Vector(512), nullable=True)
    
    # --- CONTROLE DE CACHE / ETL ---
    source_data_hash = Column(String(64), nullable=False)
    
    # --- O SEGREDO DO JSONB ---
    # Tudo o que era sub-modelo no Pydantic (AIModelMetadata), aqui vira simplesmente JSONB
    ai_metadata = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    # --- METADADOS ---
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)