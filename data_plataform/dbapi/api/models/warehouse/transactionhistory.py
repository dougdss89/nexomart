# models/warehouse.py
from sqlalchemy import Column, Integer, DateTime, Numeric, Index, text, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.models.base import Base

class TransactionHistoryModel(Base):
    __tablename__ = "transactionhistory"

    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer, ForeignKey('product.product.product_id'), nullable=False)
    
    reference_order_id = Column(Integer, ForeignKey('sales.salesorderheader.salesorder_id'), nullable=False)
    reference_order_line_id = Column(Integer, nullable=True)
    
    transaction_date = Column(DateTime(timezone=True), server_default=func.now())
    transactiontype_id = Column(Integer, ForeignKey('warehouse.transactiontype.transactiontype_id'), nullable=False)
    
    quantity = Column(Integer, nullable=False)
    
    # Custo Histórico: O Numeric(19, 4) mapeia exatamente o Decimal(19,4) do Pydantic
    actual_cost = Column(Numeric(19, 4), nullable=False)
    
    traceability_info = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
            Index('ix_traceability_info_gin', 'traceability_info', postgresql_using='gin'),
            {"schema": "warehouse"}
    )