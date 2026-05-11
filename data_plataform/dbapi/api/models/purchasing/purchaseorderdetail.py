from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Numeric
from api.config.base import Base, AuditMixin

class PurchaseOrderDetailModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderdetail"
    
    purchaseorderdetail_id = Column(String(36), primary_key=True)
    purchaseorderheader_id = Column(String(36), ForeignKey('purchasing.purchaseorderheader.purchaseorderheader_id'), nullable=False)

    product_id = Column(Integer, ForeignKey('product.product.product_id'), nullable=False)
    
    last_receipt_date = Column(DateTime(timezone=True), nullable=True)
    is_fully_received = Column(Boolean, nullable= False, default=False)
    
    due_date = Column(DateTime(timezone=True), nullable=False)
    order_qty = Column(Integer, nullable=False)
    unit_price = Column(Numeric(19,4), nullable=False)
    
    received_qty = Column(Integer, nullable=False, default=0)
    rejected_qty = Column(Integer, nullable=False, default=0)
    stocked_qty = Column(Integer, nullable=False, default=0)

    __table_args__ = {"schema": "purchasing"}