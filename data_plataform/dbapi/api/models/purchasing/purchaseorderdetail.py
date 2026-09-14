from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Numeric, Index
from api.config.base import Base, AuditMixin

class PurchaseOrderDetailModel(Base, AuditMixin):

    __tablename__ = "purchaseorderdetail"

    purchaseorderdetail_id = Column(String(36), primary_key=True)
    # FK to purchaseorderheader.purchaseorderheader_id dropped 2026-08-13: header became
    # RANGE-partitioned by order_date, which doesn't exist on this table for a composite FK.
    # Enforced by the seeder (always inserts real, just-fetched header IDs), not by the database.
    purchaseorderheader_id = Column(String(36), nullable=False)

    product_id = Column(Integer, ForeignKey('product.product.product_id'), nullable=False)
    
    last_receipt_date = Column(DateTime(timezone=True), nullable=True)
    is_fully_received = Column(Boolean, nullable= False, default=False)
    
    due_date = Column(DateTime(timezone=True), nullable=False)
    order_qty = Column(Integer, nullable=False)
    unit_price = Column(Numeric(19,4), nullable=False)
    
    received_qty = Column(Integer, nullable=False, default=0)
    rejected_qty = Column(Integer, nullable=False, default=0)
    stocked_qty = Column(Integer, nullable=False, default=0)

    __table_args__ = (
        # Backs the anti-join purchase_purchaseorderdetail.py uses to find headers still
        # pending detail lines — without this, that query degrades to a full scan on every run.
        Index("ix_purchaseorderdetail_header_id", "purchaseorderheader_id"),
        {"schema": "purchasing"},
    )