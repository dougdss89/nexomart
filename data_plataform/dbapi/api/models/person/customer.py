from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Index, Numeric
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class CustomerModel(Base):
    
    __tablename__ = "customer"
    __table_args__ = (
         Index('ix_persondemographics_gin', 'demographics', postgresql_using='gin'),
                {"schema": "person"})
    
    customer_id = Column(Integer, primary_key=True, autoincrement=True)
    
    customer_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    social_number = Column(String(15), nullable=False)
    birthdate = Column(DateTime(timezone=True), nullable=False)
    
    customer_type = Column(String(15), nullable=False)
    credit_limit = Column(Numeric(19, 4),nullable=False, default=0.0)
    
    demographics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now()) 
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)