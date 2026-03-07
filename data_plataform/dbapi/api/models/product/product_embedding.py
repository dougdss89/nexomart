# models/product.py
from sqlalchemy import Column, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector
from api.models.base import Base, AuditMixin

class ProductEmbeddingModel(Base, AuditMixin):
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
    