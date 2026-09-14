from sqlalchemy import Column, Integer, String, DateTime, text, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import Base, AuditMixin

class Product(Base, AuditMixin):

    __tablename__ = "product"

    product_id = Column(Integer, primary_key=True, autoincrement=True)
    product_name = Column(String(100), nullable=False)

    model_id = Column(Integer,
                      ForeignKey("product.productmodel.productmodel_id"),
                      nullable=False)

    category_id = Column(Integer,
                         ForeignKey("product.productcategory.category_id"),
                         nullable=False)

    sell_start_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    sell_end_date = Column(DateTime(timezone=True), nullable=True, default=None)

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