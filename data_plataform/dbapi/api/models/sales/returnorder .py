from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Index, Foreign_key, Numeric
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid


class SalesReturnOrderModel(Base):
    
    __tablename__ = "returnorder"
    __table_args__ = (
                Index('ix_inspection_data', 'inspection_data', postgresql_using="gin"),
                {"schema": "sales"}
    )
    
    return_id = Column(Integer, primary_key=True, autoincrement=True)
    requestorder_code = Column(String(20), nullable = False)
    
    salesordernumber = Column(String(20), Foreign_key='sales.salesorder.salesordernumber')
    customer_id = Column(Integer, Foreign_key = 'person.customer.customer_id')
    store_id = Column(Integer, Foreign_key = 'person.seller.seller_id')
    product_id = Column(Integer, Foreign_key = 'product.product.product_id')
    
    quantity = Column(Integer, nullable=False)
    
    return_reason = Column(String(100), nullable= False)
    return_status = Column(String(20), nullable= False)
    requested_date = Column(DateTime(timezone=True), server_default=func.now())
    return_date = Column(DateTime(timezone=True), server_default=func.now())
    end_date = Column(DateTime(timezone=True), server_default=func.now())
    
    refund_amount = Column(Numeric(19,4), nullable=False, default=0.0)
    
    inspection_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)