from api.config.base import AuditMixin, Base
from sqlalchemy import Column, Integer, String


class MarketingChannelModel(Base, AuditMixin):
    __tablename__ = "channel"
    __table_args__ = {"schema": "marketing"}

    channel_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String(50), nullable=False)
    platform_name = Column(String(50), nullable=False)
