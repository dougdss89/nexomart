from sqlalchemy import Column, Integer, Boolean, DateTime, Index, text, Foreign_key, Numeric
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class MarketingDailyPerformanceModel(Base):
    
    __tablename__ = "dailyperformance"
    __table_args__ = (
            Index('ix_platform_metrics_gin', 'platform_metrics', postgresql_using='gin'),
            {"schema": "marketing"}
    )
    
    performance_id = Column(Integer, primary_key=True, autoincrement=True)
    advertise_id = Column(Integer, nullable=False, Foreign_key='marketing.advertise.advertise_id')
    campaign_id = Column(Integer, nullable=False, Foreign_key= 'marketing.campaign.campaign_id')
    
    cost = Column(Numeric(19,4), nullable=False, default= 0.0)
    impressions = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)
    conversions = Column(Integer, nullable=False, default=0)
    revenue = Column(Numeric(19,4), nullable=False, default=0.0)
    
    platform_metrics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, nullable=False, server_default=text("true"))
    row_gruid = Column(UUID, nullable=False, default=uuid.uuid4(), server_default=text("uuid_generate_v4()"))