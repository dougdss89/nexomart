from sqlalchemy import Column, Integer, String, Index, text, cast, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from api.config.base import AuditMixin, Base

class MarketingAdGroupModel(Base, AuditMixin):
    
    __tablename__ = "marketingadgroup"

    adgroup_id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(Integer, ForeignKey('marketing.marketingcampaign.campaign_id'), nullable=False) 
    external_id = Column(String(200), nullable=False)
    adgroup_name = Column(String(200), nullable=False)
    targeting_criteria = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
        Index('ix_marketing_criteria_geo_locations_include', cast(targeting_criteria['geo_location']['include'].astext, String)),
        Index('ix_marketing_criteria_geo_locations_exclude', cast(targeting_criteria['geo_location']['exclude'].astext, String)),
        Index('ix_marketing_criteria_age_range_min', cast(targeting_criteria['age_range']['min_age'].astext, Integer)),
        Index('ix_marketing_criteria_age_range_max', cast(targeting_criteria['age_range']['max_age'].astext, Integer)),
        Index('ix_marketing_criteria_device_types',  cast(targeting_criteria['device_types'].astext, String)),
        {"schema": "marketing"}
    )
