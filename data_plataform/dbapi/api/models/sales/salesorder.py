from sqlalchemy import Column, Integer, String, DateTime, text, Index, Numeric, ForeignKey, Date
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import AuditMixin, Base

class SalesOrderModel(Base, AuditMixin):
    
    __tablename__ = "salesorder"
   
    
    salesorder_id = Column(Integer, primary_key=True, autoincrement=True)
    salesordernumber = Column(String(20), nullable=False, Unique = True)
    
    customer_id = Column(Integer,       ForeignKey("person.customer.customer_id"), nullable=False)
    saleschannel_id = Column(Integer,   ForeignKey("sales.saleschannel.saleschannel_id"), nullable=False)
    specialoffer_id = Column(Integer,   ForeignKey("sales.specialoffer.special_offer_id"), nullable=False)
    store_id = Column(Integer,          ForeignKey("person.seller.seller_id"), nullable=False)
    
    order_date = Column(DateTime(timezone=True), nullable=False)
    order_status = Column(String(), nullable=False)
    
    total_amount = Column(Numeric(19, 4), nullable=False)
    currency_code = Column(String(3), nullable=False, default="BRL")
    
    order_items = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    payment_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ =(
            
        #     Index('ix_order_items_sku', order_items['sku'].astext),
        #     Index('ix_order_items_quantity', order_items['quantity'].astext, Integer),
        #     Index('ix_order_items_seller_id', order_items['seller_id'].astext, Integer),
        #     Index('ix_order_items_product_id', order_items['product_id'].astext, Integer),
            
        #     Index('ix_order_items_delivery_info_shipdate', order_items['delivery_info']['shipdate'].astext, Date),
        #     Index('ix_order_items_delivery_info_shipping_method', order_items['delivery_info']['shipping_method'].astext),
        #     Index('ix_order_items_delivery_info_shipping_tier', order_items['delivery_info']['shipping_tier'].astext),
        #     Index('ix_order_items_delivery_info_tracking_code', order_items['delivery_info']['tracking_code'].astext),
            
            Index('ix_order_items_gin', 'order_items', postgresql_using='gin'),
            Index('ix_payment_info_gin', 'payment_info', postgresql_using='gin'),
            
            
            {"schema": "sales"})