from sqlalchemy import Column, Integer, Boolean, DateTime, String, Numeric
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class HumanResourcesJobRoleModel(Base):
    
    __tablename__ = "jobrole"
    __table_args__ = {"schema": "humanresources"}
    
    jobrole_id = Column(Integer, primary_key = True, autoincrement = True)
    jobtitle = Column(String(100), nullable = False)
    
    min_salary = Column(Numeric(19,4), nullable = False, default = 0.0)
    max_salary = Column(Numeric(19,4), nullable = True, default = 0.0)
    
    effective_startdate = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    effective_enddate = Column(DateTime(timezone=True), nullable = True, server_default=func.now())
    
    is_current = Column(Boolean, nullable = False, default = True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)