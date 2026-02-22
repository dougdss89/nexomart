from sqlalchemy import Column, Integer, String, Boolean, DateTime, Foreign_Key
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SpecialOfferProductModel(Base):
    
    __tablename__ = "specialofferproduct"
    __table_args__ = {"schema": "sales"}
    
    specialoffer_product_id = Column(Integer, primary_key=True, autoincrement=True)
    special_offer_id = Column(Integer, Foreign_key="sales.specialoffer.special_offer_id", nullable= False)
    product_id = Column(Integer, Foreign_key="product.product.product_id", nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)