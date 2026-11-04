import uuid

from api.config.base import AuditMixin, Base
from sqlalchemy import Column, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID


class CustomerEmailModel(Base, AuditMixin):
    __tablename__ = "customer_email"
    __table_args__ = (
        # Backs the anti-join person_customer_email.py uses to find customers still
        # pending an email (`LEFT JOIN ... WHERE customer_id IS NULL`) — without this,
        # that query degrades to a full scan of this table on every DAG run.
        Index("ix_customer_email_customer_id", "customer_id"),
        {"schema": "person"},
    )

    customer_email_id = Column(Integer, primary_key=True, autoincrement=True)
    # FK to customer.customer_id dropped 2026-08-13: customer became partitioned by
    # country_id, which doesn't exist on this table for a composite FK. Enforced by the
    # seeder (always inserts real, just-fetched customer_id values), not by the database.
    customer_id = Column(Integer, nullable=False)
    customer_email = Column(String(200), nullable=False)
    is_primary = Column(
        Integer, nullable=False, default=1
    )  # 1 para true, 0 para false (ou Boolean se DB suportar)
    email_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
