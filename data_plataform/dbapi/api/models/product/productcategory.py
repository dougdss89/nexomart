from sqlalchemy import Column, Integer, String, Boolean, DateTime, Numeric, Index, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class ProductCategorModel(Base):
    __tablename__ = "productcategory"
    __table_args__ = ({"schema": "product"})
    
    category_id  = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(50), nullable=False, unique= True)
    category_identifier = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    created_date = Column(DateTime(timezone=True), server_default=func.now())
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)