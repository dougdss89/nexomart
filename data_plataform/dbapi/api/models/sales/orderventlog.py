from sqlalchemy import Column, Integer, String, Boolean, DateTime, text, Index, Foreing_key
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class SalesOrderEventLogModel(Base):
    
    __tablename__ = "ordereventlog"
    __table_args__ = (
            Index('ix_event_data', 'event_data', postgresql_using='gin'),
            {"shema": "sales"}
    )
    
    event_id = Column(Integer, primary_key=True, autoincrement=True)
    salesordernumber = Column(Integer, Foreing_key= 'sales.salesorder.salesordernumber', nullable=False)
    event_type = Column(String(50), nullable=False)
    event_date = Column(DateTime(timezone=True), server_default=func.now())
    
    event_data = traceability_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)
    
