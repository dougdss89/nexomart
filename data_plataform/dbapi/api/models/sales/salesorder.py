from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Index, Numeric, Foreign_Key
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SalesOrderModel(Base):
    
    __tablename__ = "salesorder"
    __table_args__ =(
            Index('ix_order_items_gin', 'order_items', postgresql_using='gin'),
            Index('ix_payment_info_gin', 'payment_info', postgresql_using='gin'),
            {"schema": "sales"})
    
    
    salesorder_id = Column(Integer, primary_key=True, autoincrement=True)
    salesordernumber = Column(String(20), nullable=False, Unique = True)
    
    customer_id = Column(Integer, Foreign_key="person.customer.customer_id", nullable=False)
    saleschannel_id = Column(Integer, Foreign_key="sales.saleschannel.saleschannel_id", nullable=False)
    specialoffer_id = Column(Integer, Foreign_key="sales.specialoffer.special_offer_id", nullable=False)
    store_id = Column(Integer, Foreign_key="person.seller.seller_id", nullable=False)
    
    order_date = Column(DateTime(timezone=True), nullable=False)
    order_status = Column(String(), nullable=False)
    
    total_amount = Column(Numeric(19, 4), nullable=False)
    currency_code = Column(String(3), nullable=False, default="BRL")
    
    order_items = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    payment_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)