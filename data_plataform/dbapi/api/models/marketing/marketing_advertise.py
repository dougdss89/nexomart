from sqlalchemy import Column, Integer, String, Index, text, ForeignKey, cast
from sqlalchemy.dialects.postgresql import JSONB
from api.config.base import Base, AuditMixin # [FIX] Adicionado AuditMixin p/ herdar created_at, update_at, etc.

class MarketingAdvertiseModel(Base, AuditMixin): # [FIX] Herança adicionada
    
    __tablename__= "advertise"
    
    # [FIX] Nomenclaturas alinhadas 1:1 com o Schema Pydantic
    ad_id = Column(Integer, primary_key=True, autoincrement=True)
    ad_group_id = Column(Integer, ForeignKey('marketing.marketingadgroup.adgroup_id'), nullable=False )
    
    ad_name = Column(String(200), nullable=False)
    external_id = Column(String(200), nullable=False)
    
    utm_content = Column(String(200), nullable=True) # Pode ser opcional no schema (Optional[str])
    utm_medium = Column(String(200), nullable=True)  # [FIX] utm_term -> utm_medium
    
    ad_status = Column(String(20), nullable=False)   # [FIX] status -> ad_status
    
    creative_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
        Index('ix_creative_data', 'creative_data', postgresql_using='gin'),
        Index('ix_creative_data_media_type', cast(creative_data['media_type'].astext)), # [FIX] mediatype -> media_type
        
        {"schema": "marketing"}
    )