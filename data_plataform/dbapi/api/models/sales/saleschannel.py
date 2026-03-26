from sqlalchemy import Column, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB 
from api.config.base import AuditMixin, Base

class SalesChannelModel(Base, AuditMixin):
    
    __tablename__ = "saleschannel"
    
    __table_args__ = {"schema": "sales"}
    
    saleschannel_id = Column(Integer, primary_key=True, autoincrement=True)
    saleschannel_code = Column(String(20), nullable=False)
    saleschannel_name = Column(String(100), Unique=True, nullable=False)
    
    saleschannel_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

