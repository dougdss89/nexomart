from sqlalchemy import Column, Integer, Index, text,  Numeric, cast, DateTime, Date
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import Base, AuditMixin

class PurchaseOrderHeaderModel(Base, AuditMixin):
    
    __tablename__ = "purchaseorderheader"
    
    purchaseorderheader_id = Column(Integer, primary_key=True, autoincrement=True)
    
    employee_id = Column(Integer, nullable=False)
    order_date = Column(Numeric, nullable=False)
    ship_date = Column(Numeric, nullable=True)
    orderfinished_date = Column(DateTime, timezone=True, nullable=True)
    
    sub_total = Column(Numeric(19,4), nullable=False, default=0.00)
    tax_amt = Column(Numeric(19,4), nullable=False, default=0.00)
    freight = Column(Numeric(19,4), nullable=False, default=0.00)
    total_due = Column(Numeric(19,4), nullable=False, default=0.00)

    status = Column(Integer, nullable=False, default=1)  

    audit_trail = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
        Index('ix_audit_trail_action',    cast(audit_trail[1]['action'].astext)),
        Index('ix_audit_trail_user_name', cast(audit_trail[2]['user'].astext)),
        Index('ix_audit_trail_date', cast(audit_trail[0]['date'].astext, Date)),
        
        {"schema": "purchase"}
    )

