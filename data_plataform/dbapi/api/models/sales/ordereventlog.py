from sqlalchemy import Column, Integer, String, DateTime, text, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import AuditMixin, Base

class SalesOrderEventLogModel(Base, AuditMixin):
    
    __tablename__ = "ordereventlog"

    event_id = Column(Integer, primary_key=True, autoincrement=True)

    # FK to salesorder.salesorder_id dropped 2026-08-13: salesorder became RANGE-partitioned
    # by order_date, which doesn't exist on this table for a composite FK. Enforced by the
    # seeder (always inserts real, just-fetched salesorder_id values), not by the database.
    salesorder_id = Column(Integer, nullable=False)
    event_type = Column(String(50), nullable=False)
    event_date = Column(DateTime(timezone=True), server_default=func.now())
    
    event_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
            Index('ix_event_data_gin', 'event_data', postgresql_using='gin'),
            Index('ix_event_data_user_id', event_data['user_id'].astext),
            
            {"schema": "sales"}
    )
    