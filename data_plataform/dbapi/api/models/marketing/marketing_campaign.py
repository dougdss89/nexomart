
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Index, Foreign_key, Numeric, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class MarketingCampaignModel(Base):
    
    __tablename__ = "marketingcampaign"
    __table_args__ = (
        Index('ix_marketing_settings_gin', 'mktsettings', postgresql_using='gin'),
        {"schema": "marketing"}
    )
    
    campaign_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, nullable=False, Foreign_key='marketing.channel.channel_id')
    
    campaign_name = Column(String(200), nullable=False)
    utm_campaign = Column(String(200), nullable=False)
    
    budge_type = Column(String(20), nullable=False)
    total_budget = Column(Numeric(19, 4), nullable=False, default= 0.0)
    start_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    mktsettings = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    status = Column(String(20), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)