from sqlalchemy import Column, Integer, Index, text, ForeignKey, Numeric, cast, Date
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import AuditMixin, Base # [FIX] AuditMixin included

class MarketingDailyPerformanceModel(Base, AuditMixin): # [FIX] Inheritance
    
    __tablename__ = "dailyperformance"
    
    # [FIX] Nomenclaturas alinhadas com Schema e adição do nível Group
    performance_id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(Integer, ForeignKey('marketing.marketingcampaign.campaign_id'), nullable=False)  
    ad_group_id = Column(Integer, ForeignKey('marketing.marketingadgroup.adgroup_id'), nullable=False) # [FIX] Falta
    ad_id = Column(Integer, ForeignKey('marketing.advertise.ad_id'), nullable=False) # [FIX] advertise_id -> ad_id
    
    # [CRITICAL FIX] Adicionado marcador de tempo diário ausente (Time-Series DW Requirement)
    date_ref = Column(Date, nullable=False)
    
    cost = Column(Numeric(19,4), nullable=False, default= 0.0)
    impressions = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)
    conversions = Column(Integer, nullable=False, default=0)
    conversion_value = Column(Numeric(19,2), nullable=False, default=0.0) # [FIX] revenue -> conversion_value & decimal=2
    
    metrics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb")) # [FIX] platform_metrics -> metrics
    
    # [FIX] Cópia dos Índices usando chaves reais do Pydantic JSON
    __table_args__ = (
            Index('ix_performance_metrics_quality_score', cast(metrics['quality_score'].astext, Integer)),
            Index('ix_performance_metrics_likes', cast(metrics['likes'].astext, Integer)),
            Index('ix_performance_metrics_shares', cast(metrics['shares'].astext, Integer)), # Antes batia errado no quality_share
            
            {"schema": "marketing"}
    )