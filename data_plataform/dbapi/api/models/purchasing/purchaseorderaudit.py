from sqlalchemy import Column, Integer, String, ForeignKey, Index, DateTime, func
from api.config.base import Base, AuditMixin


class PurchaseOrderAuditModel(Base, AuditMixin):

    __tablename__ = "purchaseorderaudit"

    audit_id = Column(Integer, primary_key=True, autoincrement=True)
    # Table is RANGE-partitioned by created_at (migration 010), which Postgres requires to be part
    # of the primary key — so created_at (from AuditMixin) is overridden here as a composite PK
    # column. Same pattern as sales.salesorder (order_date).
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False, index=True, primary_key=True)
    # FK to purchaseorderheader.purchaseorderheader_id dropped 2026-08-13: header became
    # RANGE-partitioned by order_date, which doesn't exist on this table for a composite FK.
    # Enforced by the seeder (always inserts real, just-fetched header IDs), not by the database.
    purchaseorderheader_id = Column(String, nullable=False)
    
    action = Column(String, nullable=False)
    employee_id = Column(Integer, ForeignKey('humanresources.employee.employee_id'), nullable=False)
    obs = Column(String(200), nullable=True)

    __table_args__ = (
        # Backs the anti-join purchase_purchaseorderaudit.py uses to find headers still
        # pending an audit trail — without this, that query degrades to a full scan on every run.
        Index("ix_purchaseorderaudit_header_id", "purchaseorderheader_id"),
        {"schema": "purchasing"},
    )