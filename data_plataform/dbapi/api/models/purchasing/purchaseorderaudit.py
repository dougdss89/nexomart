from sqlalchemy import Column, Integer, String, Foreign_key
from api.models.base import Base, AuditMixin


class PurchaseOrderAuditModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderaudit"
    
    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    purchaseorderheader_id = Column(String, Foreign_key('purchase.purchaseorderheader.purchaseorderheader_id'), nullable=False)
    
    action = Column(String, nullable=False)
    user_id = Column(Integer, Foreign_key('humanresources.employee.employee_id'), nullable=False)
    obs = Column(String(200), nullable=False)