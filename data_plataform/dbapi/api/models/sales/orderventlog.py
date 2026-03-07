from sqlalchemy import Column, Integer, String, DateTime, text, Index, Foreing_key
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.models.base import AuditMixin, Base

class SalesOrderEventLogModel(Base, AuditMixin):
    
    __tablename__ = "ordereventlog"

    event_id = Column(Integer, primary_key=True, autoincrement=True)
    
    salesordernumber = Column(Integer, Foreing_key('sales.salesorder.salesordernumber'), nullable=False)
    event_type = Column(String(50), nullable=False)
    event_date = Column(DateTime(timezone=True), server_default=func.now())
    
    event_data = traceability_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    
    __table_args__ = (
            Index('ix_event_data', 'event_data', postgresql_using='gin'),
            Index('ix_event_data_user_id', event_data['user_id'].astext, Integer),
            
            {"shema": "sales"}
    )
    