from sqlalchemy import Column, Integer, String, Index, text, Foreign_key, cast
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import Base

class MarketingAdvertiseModel(Base):
    
    __tablename__= "advertise"

    
    advertise_id = Column(Integer, primary_key=True, autoincrement=True)
    adgroup_id = Column(Integer, Foreign_key('marketing.marketingadgroup.adgroup_id'), nullable=False )
    
    advertise_name = Column(String(200), nullable=False)
    external_id = Column(String(200), nullable=False)
    
    utm_content = Column(String(200), nullable=False)
    utm_term = Column(String(200), nullable=False)
    
    creative_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    status = Column(String(20), nullable=False)
    
    __table_args__ = (
        Index('ix_creative_data', 'creative_data', postgresql_using='gin'),
        Index('ix_creative_data_media_type', cast(creative_data['mediatype'].astext)),
        
        {"schema": "marketing"})