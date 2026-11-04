from api.config.base import AuditMixin, Base
from sqlalchemy import Column, Index, Integer, Numeric, String, cast, text
from sqlalchemy.dialects.postgresql import JSONB


class WarehouseLocationModel(Base, AuditMixin):
    __tablename__ = "warehouselocation"

    warehouselocation_id = Column(Integer, primary_key=True, autoincrement=True)
    warehousename = Column(String(50), nullable=False)

    costrate = Column(Numeric(19, 4), nullable=False, default=0.00)
    availability = Column(Numeric(19, 4), nullable=False, default=0.00)

    capabilities = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    geo_location = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    __table_args__ = (
        Index("ix_warehouse_capabilities_type", cast(capabilities["warehousetype"].astext, String)),
        Index("ix_geo_location_countryid", cast(geo_location["countryid"].astext, Integer)),
        Index("ix_geo_location_contryname", cast(geo_location["countryname"].astext, String)),
        Index(
            "ix_geo_location_stateprovinceid", cast(geo_location["stateprovinceid"].astext, Integer)
        ),
        Index(
            "ix_geo_location_stateprovincename",
            cast(geo_location["stateprovincename"].astext, String),
        ),
        {"schema": "warehouse"},
    )
