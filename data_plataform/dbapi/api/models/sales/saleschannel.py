from sqlalchemy import Column, Integer, String, Boolean, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SalesChannelModel(Base):
    
    __tablename__ = "saleschannel"
    __table_args__ = {"schema": "sales"}
    
    saleschannel_id = Column(Integer, primary_key=True, autoincrement=True)
    saleschannel_code = Column(String(20), nullable=False)
    saleschannel_name = Column(String(100), Unique=True, nullable=False)
    
    saleschannel_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
