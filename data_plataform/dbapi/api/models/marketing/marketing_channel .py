from sqlalchemy import Column, Integer, String

from api.models.base import AuditMixin, Base

class MarketingChannelModel(Base, AuditMixin):
    
    __tablename__ = "channel"
    __table_args__ = {"schema": "marketing"}
    
    channel_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String(50), nullable=False)
    platform_name = Column(String(50), nullable=False)
    
    