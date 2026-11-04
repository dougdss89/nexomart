# models/warehouse.py
from api.config.base import AuditMixin, Base
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, Numeric, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func


class TransactionHistoryModel(Base, AuditMixin):
    """warehouse.transactionhistory is RANGE-partitioned by transaction_date (2026-08-13,
    monthly partitions warehouse.transactionhistory_2026_05..._09 plus a DEFAULT catch-all).
    Postgres requires the partition key in every unique/PK constraint, so the PK became
    (transaction_id, transaction_date).

    No table FKs *into* transactionhistory (it's a leaf fact table), so unlike customer/
    salesorder this migration didn't need to touch any dependent model.
    """

    __tablename__ = "transactionhistory"

    transaction_id = Column(String(36), primary_key=True)
    product_id = Column(Integer, ForeignKey("product.product.product_id"), nullable=False)

    # FK to salesorder.salesorder_id dropped 2026-08-13: salesorder became RANGE-partitioned
    # by order_date, which doesn't exist on this table for a composite FK. Enforced by the
    # seeder (always inserts real, just-fetched salesorder_id values), not by the database.
    reference_order_id = Column(Integer, nullable=False)
    reference_order_line_id = Column(Integer, nullable=True)

    transaction_date = Column(DateTime(timezone=True), primary_key=True, server_default=func.now())
    transactiontype_id = Column(
        Integer, ForeignKey("warehouse.transactiontype.transactiontype_id"), nullable=False
    )

    quantity = Column(Integer, nullable=False)

    # Historical Cost: Numeric(19, 4) maps exactly to Pydantic's Decimal(19,4)
    actual_cost = Column(Numeric(19, 4), nullable=False)

    traceability_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    __table_args__ = (
        Index("ix_traceability_info_gin", "traceability_info", postgresql_using="gin"),
        {"schema": "warehouse"},
    )
