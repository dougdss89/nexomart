from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import AuditMixin, Base
import uuid

class ProductModel(Base, AuditMixin):
    __tablename__ = "productmodel"
    
    __table_args__ = ({"schema": "product"})
    
    productmodel_id = Column(Integer, primary_key=True, autoincrement=True)
    
    productmodel_name = Column(String(50), nullable=False)
    modelidentifier = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    created_date = Column(DateTime(timezone=True), server_default=func.now())
    
    
    