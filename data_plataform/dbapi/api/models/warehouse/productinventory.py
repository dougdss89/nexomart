from datetime import date
from sqlalchemy import Column, Integer, String, text, Index, cast, ForeignKey, DateTime
from sqlalchemy.sql import func
from api.config.base import Base, AuditMixin
from sqlalchemy.dialects.postgresql import JSONB

class WarehouseProductInventoryModel(Base, AuditMixin):
    
    __tablename__ = "productinventory"
    
    inventory_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('product.product.product_id'),nullable=False)
    warehouselocation_id = Column(Integer, ForeignKey('warehouse.warehouselocation.warehouselocation_id'), nullable=False)
    
    quantity = Column(Integer, nullable=False, default=0)
    bin_code = Column(String(50), nullable=False)
    last_restock_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    attributes = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    modified_date = Column(DateTime(timezone=True), nullable=True, default=None,server_default=func.now())
    
    __table_args__ = (
        Index('ix_inventory_attributes_expiration_date', cast(attributes['expiration_date'].astext, date)),
        Index('ix_inventory_attributes_manufacturing_date', cast(attributes['manufacturing_date'].astext, date)),
        
        {'schema': 'warehouse'}
        
        )