
from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Index, Numeric
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SellerModel(Base):
    
    __tablename__ = "seller"
    __table_args__ = (
            Index('ix_seller_demographics', 'demographics', postgresql_using='gin'),
            {"schema": "person"})
    
    seller_id = Column(Integer, primary_key=True, autoincrement=True)
    seller_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    social_number = Column(String(15), nullable=False)
    
    opened_date = Column(DateTime(timezone=True), nullable=False, serverdefault=func.now())
    
    commission_rate = Column(Numeric(19, 4), nullable=False, default=0.0)
    
    demographics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    country_id = Column(Integer, nullable=False, Foreign_key="person.countryregion.country_id")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)