from sqlalchemy import Column, Integer, Foreign_key, DateTime, Boolean, Numeric
from api.models.base import Base, AuditMixin

class PurchaseOrderDetailModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderdetail"
    
    purchaseorderdetail_id = Column(Integer, primary_key=True, autoincrement=True)
    purchaseorderheader_id = Column(Integer, Foreign_key('purchase.purchaseorderheader.purchaseorderheader_id'), nullable=False)
    
    product_id = Column(Integer, Foreign_key('production.product.product_id'), nullable=False)
    
    lastreceipt_date = Column(DateTime, timezone= True, nullable=True)
    is_fully_received = Column(Boolean, nullable= False, default=False)
    
    duedate = Column(DateTime, timezone=True, nullable=False)
    order_qty = Column(Integer, nullable=False)
    unitprice = Column(Numeric(19,4), nullable=False)
    
    received_qty = Column(Integer, nullable=False, default = 0)
    rejected_qty = Column(Integer, nullable=False, default = 0)
    stocked_qty = Column(Integer, nullable=False, default = 0)
    
    modified_date = Column(DateTime, timezone=True, nullable=False)
    
    