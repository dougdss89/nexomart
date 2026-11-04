from api.config.base import AuditMixin, Base
from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.sql import func


class SpecialOfferModel(Base, AuditMixin):
    __tablename__ = "specialoffer"
    __table_args__ = {"schema": "sales"}

    special_offer_id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String(100), nullable=False)
    discount_pct = Column(Integer, nullable=False)
    discount_type = Column(String(100), nullable=False)
    category = Column(String(100), nullable=False)

    min_quantity = Column(Integer, nullable=False)
    max_quantity = Column(Integer, nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    end_date = Column(
        DateTime(timezone=True), nullable=True, default=None, server_default=func.now()
    )
