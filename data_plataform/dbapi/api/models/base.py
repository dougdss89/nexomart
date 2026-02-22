# app/models/base.py
import uuid
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Boolean, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from datetime import datetime, date

class Base(DeclarativeBase):
    
    def __repr__(self):
        primary_key_column = self.__mapper__.primary_key[0].name
        primary_key_value = getattr(self, primary_key_column)
        return f"<{self.__class__.__name__}({primary_key_column}={primary_key_value})>"
    
    def to_dict(self):
        result = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, uuid.UUID):
                result[column.name] = str(value)
            elif isinstance(value, (datetime, date)):
                result[column.name] = value.isoformat()
            else:
                result[column.name] = value
        return result

# --- AQUI ESTÁ A MÁGICA ---
class AuditMixin:
    """
    Mixin para injetar colunas de rastreabilidade e auditoria.
    Basta herdar desta classe para a tabela ganhar esses 4 campos automaticamente.
    """
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)