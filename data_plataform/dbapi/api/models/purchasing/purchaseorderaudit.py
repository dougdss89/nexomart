from sqlalchemy import Column, Integer, String, ForeignKey
from api.models.base import Base, AuditMixin


class PurchaseOrderAuditModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderaudit"
    
    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    purchaseorderheader_id = Column(String, ForeignKey('purchase.purchaseorderheader.purchaseorderheader_id'), nullable=False)
    
    action = Column(String, nullable=False)
    employee_id = Column(Integer, ForeignKey('humanresources.employee.employee_id'), nullable=False)
    obs = Column(String(200), nullable=True)