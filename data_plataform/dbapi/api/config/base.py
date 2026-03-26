# app/models/base.py
import uuid
from typing import Any, Dict, Type, TypeVar
from sqlalchemy.orm import DeclarativeBase, declared_attr
from sqlalchemy import Column, Boolean, DateTime, Integer, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from datetime import datetime, date

T = TypeVar("T", bound="Base")

class Base(DeclarativeBase):
    """
    Base aprimorada com utilitários de serialização e integração Pydantic.
    """
    
    def __repr__(self):
        try:
            primary_key_column = self.__mapper__.primary_key[0].name
            primary_key_value = getattr(self, primary_key_column)
            return f"<{self.__class__.__name__}({primary_key_column}={primary_key_value})>"
        except Exception:
            return f"<{self.__class__.__name__} (uninitialized)>"
    
    def to_dict(self, exclude: set = None) -> Dict[str, Any]:
        """
        Converte o modelo em um dicionário purificado.
        """
        exclude = exclude or set()
        result = {}
        for column in self.__table__.columns:
            if column.name in exclude:
                continue
            value = getattr(self, column.name)
            if isinstance(value, uuid.UUID):
                result[column.name] = str(value)
            elif isinstance(value, (datetime, date)):
                result[column.name] = value.isoformat()
            else:
                result[column.name] = value
        return result

    def update_from_dict(self, data: Dict[str, Any], ignore_missing: bool = True):
        """
        Atualiza atributos do modelo a partir de um dicionário.
        """
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
            elif not ignore_missing:
                raise AttributeError(f"{self.__class__.__name__} não possui o atributo {key}")

    @classmethod
    def from_pydantic(cls: Type[T], schema: Any) -> T:
        """
        Cria uma instância do modelo a partir de um schema Pydantic,
        garantindo que os defaults do SQLAlchemy sejam aplicados.
        """
        data = schema.model_dump() if hasattr(schema, "model_dump") else schema.dict()
        instance = cls(**data)
        
        # Proativamente inicializa colunas que têm default e estão como None
        for column in cls.__table__.columns:
            if getattr(instance, column.name) is None and column.default is not None:
                if column.default.is_callable:
                    setattr(instance, column.name, column.default.arg(None))
                elif column.default.is_scalar:
                    setattr(instance, column.name, column.default.arg)
        
        return instance

class AuditMixin:
    """
    Mixin para injetar colunas de rastreabilidade e auditoria com performance aprimorada.
    """
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=True, default=None, onupdate=func.now())
    is_active = Column(Boolean, default=True, nullable=False)
    row_version = Column(Integer, default=1, nullable=False)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)

    @declared_attr
    def ix_created_at(cls):
        return Index(f"ix_{cls.__tablename__}_created_at", "created_at")

    def soft_delete(self):
        """
        Realiza a inativação lógica do registro.
        """
        self.is_active = False