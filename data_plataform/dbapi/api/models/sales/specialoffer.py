from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SpecialOfferModel(Base):
    
    __tablename__ = "specialoffer"
    __table_args__ = ({"schema": "sales"})
    
    special_offer_id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(String(100), nullable=False)
    discount_pct = Column(Integer, nullable=False)
    type = Column(String(100), nullable=False)
    category = Column(String(100), nullable=False)
    
    min_quantity = Column(Integer, nullable=False)
    max_quantity = Column(Integer, nullable=False)
    start_date = Column(DateTime(timezone=True), nullable=False)
    end_date = Column(DateTime(timezone=True), nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)