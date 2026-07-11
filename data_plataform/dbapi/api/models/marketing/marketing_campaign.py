
from sqlalchemy import Column, Integer, String, DateTime, Index, ForeignKey, Numeric, text, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import Base, AuditMixin

class MarketingCampaignModel(Base, AuditMixin):
    
    __tablename__ = "marketingcampaign"

    
    campaign_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(Integer, ForeignKey('marketing.channel.channel_id'), nullable=False)
    
    campaign_name = Column(String(200), nullable=False)
    utm_campaign = Column(String(200), nullable=False)
    
    budget_type = Column(String(20), nullable=False)
    total_budget = Column(Numeric(19, 4), nullable=False, default= 0.0)
    start_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    end_date = Column(DateTime(timezone=True), nullable=True, default=None)

    status = Column(String(20), nullable=False, default='planned')

    mktsettings = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    
    __table_args__ = (
        Index('ix_marketing_settings_gin', 'mktsettings', postgresql_using='gin'),
        
        Index('ix_marketing_settings_objective', cast(mktsettings['objective'].astext, String)),
        Index('ix_marketing_settings_bid_strategies', cast(mktsettings['bidding_strategy'].astext, String)),
        
        {"schema": "marketing"}
    )