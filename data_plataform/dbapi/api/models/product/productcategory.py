import uuid

from api.config.base import AuditMixin, Base
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func


class ProductCategoryModel(Base, AuditMixin):
    __tablename__ = "productcategory"

    __table_args__ = {"schema": "product"}

    category_id = Column(Integer, primary_key=True, autoincrement=True)
    category_name = Column(String(50), nullable=False, unique=True)
    category_identifier = Column(
        UUID(as_uuid=True), default=uuid.uuid4, unique=True, nullable=False
    )
    created_date = Column(DateTime(timezone=True), server_default=func.now())

    parent_category_id = Column(
        Integer, ForeignKey("product.productcategory.category_id"), nullable=True
    )
