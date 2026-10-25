from api.config.base import AuditMixin, Base
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func


class WarehouseTransactionType(Base, AuditMixin):
    __tablename__ = "transactiontype"

    transactiontype_id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_code = Column(String(2), nullable=False, unique=True)
    transaction_name = Column(String(50), nullable=False)

    description = Column(String(255), nullable=False)

    action_effect = Column(String(20), nullable=False)

    valid_from = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    valid_to = Column(DateTime(timezone=True), nullable=True, default=None)

    __table_args__ = {"schema": "warehouse"}
