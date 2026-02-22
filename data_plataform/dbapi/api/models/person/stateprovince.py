from sqlalchemy import Column, Integer, String, Boolean, DateTime, Foreign_Key
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid


class StateProvinceModel(Base):
    
    __tablename__ = "stateprovince"
    __table_args__ = ({"schema": "person"})
    
    state_province_id = Column(Integer, primary_key=True, autoincrement=True)
    state_province_name = Column(String(100), nullable=False)
    state_province_code = Column(String(3), nullable=False)
    country_id = Column(Integer, Foreign_Key("person.countryregion.country_id"), nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
 