from api.config.base import (  # [FIX] Added AuditMixin to inherit created_at, updated_at, etc.
    AuditMixin,
    Base,
)
from sqlalchemy import Column, ForeignKey, Index, Integer, String, cast, text
from sqlalchemy.dialects.postgresql import JSONB


class MarketingAdvertiseModel(Base, AuditMixin):  # [FIX] Inheritance added
    __tablename__ = "advertise"

    # [FIX] Naming aligned 1:1 with the Pydantic Schema
    ad_id = Column(Integer, primary_key=True, autoincrement=True)
    ad_group_id = Column(
        Integer, ForeignKey("marketing.marketingadgroup.ad_group_id"), nullable=False
    )

    ad_name = Column(String(200), nullable=False)
    external_id = Column(String(200), nullable=False)

    utm_content = Column(
        String(200), nullable=True
    )  # Can be optional in the schema (Optional[str])
    utm_medium = Column(String(200), nullable=True)  # [FIX] utm_term -> utm_medium

    ad_status = Column(String(20), nullable=False)  # [FIX] status -> ad_status

    creative_data = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    __table_args__ = (
        Index("ix_creative_data", "creative_data", postgresql_using="gin"),
        Index(
            "ix_creative_data_media_type", cast(creative_data["media_type"].astext, String)
        ),  # [FIX] mediatype -> media_type
        {"schema": "marketing"},
    )
