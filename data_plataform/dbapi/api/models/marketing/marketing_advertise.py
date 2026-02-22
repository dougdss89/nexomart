from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index, text, Foreign_key
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class MarketingAdvertiseModel(Base):
    
    __tablename__= "advertise"
    __table_args__ = (
        Index('ix_creative_data', 'creative_data', postgresql_using='gin'),
        {"schema": "marketing"})
    
    advertise_id = Column(Integer, primary_key=True, autoincrement=True)
    adgroup_id = Column(Integer, nullable=False, Foreign_key='marketing.marketingadgroup.adgroup_id')
    
    advertise_name = Column(String(200), nullable=False)
    external_id = Column(String(200), nullable=False)
    
    utm_content = Column(String(200), nullable=False)
    utm_term = Column(String(200), nullable=False)
    
    creative_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    status = Column(String(20), nullable=False)
    
    created_at  = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    