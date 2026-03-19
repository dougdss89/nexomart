from sqlalchemy import Column, Integer, String, DateTime, text, Index, ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.models.base import AuditMixin, Base

class SalesReturnOrderModel(Base, AuditMixin):
    
    __tablename__ = "returnorder"
    
    return_id = Column(Integer, primary_key=True, autoincrement=True)
    requestorder_code = Column(String(20), nullable = False)
    
    salesordernumber = Column(String(20), ForeignKey('sales.salesorder.salesordernumber'), nullable=False)
    
    customer_id = Column(Integer, ForeignKey( 'person.customer.customer_id'), nullable=False)
    store_id = Column(Integer,    ForeignKey( 'person.seller.seller_id'),     nullable=False)
    product_id = Column(Integer,  ForeignKey( 'product.product.product_id'),  nullable=False)
    
    quantity = Column(Integer, nullable=False)
    
    return_reason = Column(String(100), nullable= False)
    return_status = Column(String(20), nullable= False)
    requested_date = Column(DateTime(timezone=True), server_default=func.now())
    return_date = Column(DateTime(timezone=True), nullable=True, default=None, server_default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=True, default=None,    server_default=func.now())
    
    refund_amount = Column(Numeric(19,4), nullable=False, default=0.0)
    
    inspection_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
                Index('ix_inspection_data', 'inspection_data', postgresql_using="gin"),
                
                {"schema": "sales"}
    )