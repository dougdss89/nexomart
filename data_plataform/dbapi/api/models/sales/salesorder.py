from sqlalchemy import Column, Integer, String, DateTime, text, Index, Numeric, ForeignKey, Date
from sqlalchemy.dialects.postgresql import JSONB
from api.config.base import AuditMixin, Base

class SalesOrderModel(Base, AuditMixin):
    """sales.salesorder is RANGE-partitioned by order_date (2026-08-13, monthly partitions
    sales.salesorder_2026_06..._10 plus a DEFAULT catch-all). Postgres requires the
    partition key in every unique/PK constraint, so the PK became (salesorder_id,
    order_date) and salesordernumber's uniqueness became (salesordernumber, order_date).

    FK to customer.customer_id dropped: customer became partitioned by country_id in the
    same migration, which doesn't exist here for a composite FK. ordereventlog.salesorder_id,
    returnorder.salesordernumber, and transactionhistory.reference_order_id (the tables that
    used to FK *into* salesorder) lost their FKs for the mirror reason — order_date isn't on
    those tables either. All of these are enforced by the seeders (always insert real,
    just-fetched values), not by the database.
    """

    __tablename__ = "salesorder"


    salesorder_id = Column(Integer, primary_key=True, autoincrement=True)
    salesordernumber = Column(String(20), nullable=False)

    customer_id = Column(Integer, nullable=False)
    saleschannel_id = Column(Integer,   ForeignKey("sales.saleschannel.saleschannel_id"), nullable=False)
    specialoffer_id = Column(Integer,   ForeignKey("sales.specialoffer.special_offer_id"), nullable=False)
    store_id = Column(Integer,          ForeignKey("person.seller.seller_id"), nullable=False)

    order_date = Column(DateTime(timezone=True), primary_key=True, nullable=False)
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
            
        # ix_order_items_gin / ix_payment_info_gin dropped 2026-08-13: together they were
        # 24.8% of shared_buffers with no confirmed real query using them (checked Trino/dbt
        # first). Add back pointually via a targeted expression index (see the commented
        # sku/quantity/etc. examples above) if a real query needs to search inside these
        # JSONB columns — a full-column GIN index is expensive for how rarely order_items/
        # payment_info are queried by contents rather than by order_id.

            
            {"schema": "sales"})