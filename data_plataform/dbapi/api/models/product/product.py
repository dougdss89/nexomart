from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Foreign_Key
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class ProductModel(Base):
    
    __tablename__ = "product"
    __table_args__ = ({"schema": "product"})
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(100), nullable=False)
    
    model_id = Column(Integer, 
                      Foreign_Key("productmodel.productmodel_id"),
                      nullable=False)
    
    category_id = Column(Integer, 
                         Foreign_Key("productcategory.category_id"),
                         nullable=False)
    
    productdetails = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active =  Column(Boolean, default=True)
    row_guid  =   Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    #relationship
    # model = relationship("ProductModel")
    # category = relationship("ProductCategory")
    