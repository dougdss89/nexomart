from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class MarketingChannelModel(Base):
    
    __tablename__ = "channel"
    __table_args__ = {"schema": "marketing"}
    
    channel_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_name = Column(String(50), nullable=False)
    platform_name = Column(String(50), nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    