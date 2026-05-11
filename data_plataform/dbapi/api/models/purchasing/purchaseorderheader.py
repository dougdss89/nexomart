from sqlalchemy import Column, Integer, String, Index, text, Numeric, cast, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from api.config.base import Base, AuditMixin

class PurchaseOrderHeaderModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderheader"
    
    purchaseorderheader_id = Column(String(36), primary_key=True)
    
    vendor_id = Column(Integer, nullable=False)
    employee_id = Column(Integer, nullable=False)

    order_date = Column(DateTime(timezone=True), nullable=False)
    ship_date = Column(DateTime(timezone=True), nullable=True)
    orderfinished_date = Column(DateTime(timezone=True), nullable=True)
    
    sub_total = Column(Numeric(19,4), nullable=False, default=0.00)
    tax_amt = Column(Numeric(19,4), nullable=False, default=0.00)
    freight = Column(Numeric(19,4), nullable=False, default=0.00)
    total_due = Column(Numeric(19,4), nullable=False, default=0.00)

    status = Column(String(20), nullable=False, default='Pending')  

    audit_trail = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    
    __table_args__ = (
        Index('ix_audit_trail_action',    cast(audit_trail[1]['action'].astext, String)),
        Index('ix_audit_trail_user_name', cast(audit_trail[2]['user'].astext, String)),
        Index('ix_audit_trail_date', cast(audit_trail[0]['date'].astext, String)),
        
        {"schema": "purchasing"}
    )

