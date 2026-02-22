from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class MarketingAdGroupModel(Base):
    
    __tablename__ = "marketingadgroup"
    __table_args__ = (
        Index('ix_marketing_criteria_gin', 'targeting_criteria', postgresql_using='gin'),
        {"schema": "marketing"}
    )
    
    adgroup_id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(Integer, nullable=False, Foreign_key='marketing.marketingcampaign.campaign_id')
    external_id = Column(String(200), nullable=False)
    
    adgroup_name = Column(String(200), nullable=False)
    
    targeting_criteria = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
