from sqlalchemy import Column, Integer, String, Index, text, Numeric, cast, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from api.config.base import Base, AuditMixin

class PurchaseOrderHeaderModel(Base, AuditMixin):
    """purchasing.purchaseorderheader is RANGE-partitioned by order_date (2026-08-13, monthly
    partitions purchasing.purchaseorderheader_2025_08..._2026_09 plus a DEFAULT catch-all —
    the seeder draws order_date up to 365 days back, so this one spans a full year unlike
    salesorder/transactionhistory). Postgres requires the partition key in every unique/PK
    constraint, so the PK became (purchaseorderheader_id, order_date).

    purchaseorderdetail.purchaseorderheader_id and purchaseorderaudit.purchaseorderheader_id
    (the tables that used to FK *into* this one) lost their FKs for the mirror reason —
    order_date isn't on either. Enforced by the seeders (always insert real, just-fetched
    values), not by the database.
    """

    __tablename__ = "purchaseorderheader"

    purchaseorderheader_id = Column(String(36), primary_key=True)

    vendor_id = Column(Integer, ForeignKey('purchasing.vendor.vendor_id'), nullable=False)
    employee_id = Column(Integer, ForeignKey('humanresources.employee.employee_id'), nullable=False)

    order_date = Column(DateTime(timezone=True), primary_key=True, nullable=False)
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

