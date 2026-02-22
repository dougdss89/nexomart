from sqlalchemy import Column, Integer, String, Boolean, DateTime, Foreign_Key
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class CustomerEmailModel(Base):
    
    __tablename__ = "customer_email"
    __table_args__ = ({"schema": "person"})
    
    customer_email_id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer, 
                         Foreign_Key("person.customer.customer_id"),
                         nullable = False )
    customer_email = Column(String(200), nullable=False)
    email_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_guid=True), default=uuid.uuid4, unique=True, nullable=False)
    