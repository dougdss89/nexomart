from sqlalchemy import Column, Integer, Index, text, ForeignKey, Numeric, cast
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import AuditMixin, Base

class MarketingDailyPerformanceModel(Base, AuditMixin):
    
    __tablename__ = "dailyperformance"
    
    performance_id = Column(Integer, primary_key=True, autoincrement=True)
    advertise_id = Column(Integer, ForeignKey('marketing.advertise.advertise_id'), nullable=False)  
    campaign_id = Column(Integer, ForeignKey('marketing.campaign.campaign_id'), nullable=False)  
    
    cost = Column(Numeric(19,4), nullable=False, default= 0.0)
    impressions = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)
    conversions = Column(Integer, nullable=False, default=0)
    revenue = Column(Numeric(19,4), nullable=False, default=0.0)
    
    platform_metrics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
            Index('ix_platform_metrics_quality_score', cast(platform_metrics['qualityscore'].astext, Integer)),
            Index('ix_platform_metrics_likes', cast(platform_metrics['likes'].astext, Integer)),
            Index('ix_platform_metrics_share', cast(platform_metrics['qualityscore'].astext, Integer)),
            
            {"schema": "marketing"}
    )