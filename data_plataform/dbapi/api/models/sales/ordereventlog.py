from sqlalchemy import Column, Integer, String, DateTime, text, Index, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import AuditMixin, Base

class SalesOrderEventLogModel(Base, AuditMixin):
    
    __tablename__ = "ordereventlog"

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    
    salesorder_id = Column(Integer, ForeignKey('sales.salesorder.salesorder_id'), nullable=False)
    event_type = Column(String(50), nullable=False)
    event_date = Column(DateTime(timezone=True), server_default=func.now())
    
    event_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
            Index('ix_event_data_gin', 'event_data', postgresql_using='gin'),
            Index('ix_event_data_user_id', event_data['user_id'].astext),
            
            {"schema": "sales"}
    )
    