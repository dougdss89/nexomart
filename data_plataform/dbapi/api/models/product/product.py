from sqlalchemy import Column, Integer, String, text, Foreign_Key, Index
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import Base, AuditMixin

class ProductModel(Base, AuditMixin):
    
    __tablename__ = "product"
    
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(100), nullable=False)
    
    model_id = Column(Integer, 
                      Foreign_Key("productmodel.productmodel_id"),
                      nullable=False)
    
    category_id = Column(Integer, 
                         Foreign_Key("productcategory.category_id"),
                         nullable=False)
    
    productdetails = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    
    __table_args__ = (
        
        Index('ix_product_product_details_color', productdetails['details']['color'].astext),
        Index('ix_product_product_details_size', productdetails['details']['size'].astext),
        Index('ix_product_product_details_product_price', productdetails['details']['product_price'].astext),
        Index('ix_product_product_details_product_brand', productdetails['details']['product_brand'].astext),
        Index('ix_product_product_details_product_sku', productdetails['details']['product_sku'].astext),
        
        #GIN para generalistas
        Index('ix_product_product_details_gin', productdetails, postgresql_using='gin'),

        {"schema": "product"})