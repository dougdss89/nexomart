import uuid
from datetime import date, datetime
from typing import Any, TypeVar

from sqlalchemy import Boolean, Column, DateTime, Integer
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func

T = TypeVar("T", bound="Base")


class Base(DeclarativeBase):
    def __repr__(self) -> str:
        try:
            pk_col = self.__mapper__.primary_key[0].name
            return f"<{self.__class__.__name__}({pk_col}={getattr(self, pk_col)})>"
        except Exception:
            return f"<{self.__class__.__name__} (uninitialized)>"

    def to_dict(self, exclude: set = None) -> dict[str, Any]:
        exclude = exclude or set()
        result = {}
        for col in self.__table__.columns:
            if col.name in exclude:
                continue
            value = getattr(self, col.name)
            if isinstance(value, uuid.UUID):
                result[col.name] = str(value)
            elif isinstance(value, (datetime, date)):
                result[col.name] = value.isoformat()
            else:
                result[col.name] = value
        return result

    def update_from_dict(self, data: dict[str, Any], ignore_missing: bool = True) -> None:
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
            elif not ignore_missing:
                raise AttributeError(f"{self.__class__.__name__} has no attribute '{key}'")

    @classmethod
    def from_pydantic(cls: type[T], schema: Any) -> T:
        data = schema.model_dump() if hasattr(schema, "model_dump") else schema.dict()
        instance = cls(**data)
        for col in cls.__table__.columns:
            if getattr(instance, col.name) is None and col.default is not None:
                if col.default.is_callable:
                    setattr(instance, col.name, col.default.arg(None))
                elif col.default.is_scalar:
                    setattr(instance, col.name, col.default.arg)
        return instance


class AuditMixin:
    """Injects audit columns (created_at, updated_at, is_active, row_version, row_guid)."""

    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    updated_at = Column(DateTime(timezone=True), nullable=True, default=None, onupdate=func.now())
    is_active = Column(Boolean, default=True, nullable=False)
    row_version = Column(Integer, default=1, nullable=False)
    row_guid = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False)

    def soft_delete(self) -> None:
        self.is_active = False
