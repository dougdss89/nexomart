# models/warehouse.py
from sqlalchemy import Column, Integer, Boolean, DateTime, Numeric, Index, text, Foreign_key
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.sql import func
from api.models.base import Base
import uuid

class TransactionHistoryModel(Base):
    __tablename__ = "transactionhistory"
    __table_args__ = (
            Index('ix_traceability_info_gin', 'traceability_info', postgresql_using='gin'),
            {"schema": "warehouse"}
    )

    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, Foreign_key('product.product.product_id'), nullable=False)
    
    reference_order_id = Column(Integer, Foreign_key('sales.salesorderheader.salesorder_id'), nullable=False)
    reference_order_line_id = Column(Integer, nullable=True)
    
    transaction_date = Column(DateTime(timezone=True), server_default=func.now())
    transactiontype_id = Column(Integer, Foreign_key('warehouse.transactiontype.transactiontype_id'), nullable=False)
    
    quantity = Column(Integer, nullable=False)
    
    # Custo Histórico: O Numeric(19, 4) mapeia exatamente o Decimal(19,4) do Pydantic
    actual_cost = Column(Numeric(19, 4), nullable=False)
    
    # --- RASTREABILIDADE (JSONB) ---
    # Aqui vai o TraceabilityInfo (Motivo, Usuário, IoT Scanner, etc)
    traceability_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)