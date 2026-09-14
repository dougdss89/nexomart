from sqlalchemy import Column, Integer, String, DateTime, text, Index, ForeignKey, Numeric
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import AuditMixin, Base

class SalesReturnOrderModel(Base, AuditMixin):
    
    __tablename__ = "returnorder"
    
    return_id = Column(String(36), primary_key=True)
    requestorder_code = Column(String(20), nullable = False)
    
    # FKs to salesorder.salesordernumber and customer.customer_id dropped 2026-08-13: both
    # targets became partitioned tables (salesorder by order_date, customer by country_id),
    # and neither partition key exists on this table to form a composite FK. Referential
    # integrity here is enforced by the seeder (always inserts real, just-fetched values),
    # not by the database.
    salesordernumber = Column(String(20), nullable=False)

    customer_id = Column(Integer, nullable=False)
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