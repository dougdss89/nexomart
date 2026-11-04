from api.config.base import AuditMixin, Base
from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, Numeric, String, cast, text
from sqlalchemy.dialects.postgresql import JSONB


class CustomerModel(Base, AuditMixin):
    """person.customer is LIST-partitioned by country_id (2026-08-13, one partition per
    canonical country + a DEFAULT catch-all — see person.customer_br/_us/_ca/.../_other in
    the DB). Postgres requires the partition key in every unique/PK constraint, so
    customer_id alone is no longer sufficient as the PK — it's (customer_id, country_id).
    customer_id itself is still globally unique (it's the same autoincrement sequence as
    before), so this doesn't change how the seeder generates or uses it.

    Tables that used to FK to customer.customer_id (customer_email, sales.returnorder,
    sales.salesorder) had that constraint dropped rather than turned into a composite FK —
    propagating country_id into three more tables was out of scope for this migration.
    Referential integrity there is enforced by the seeders (they only ever insert real,
    just-fetched customer_id values), not by the database.
    """

    __tablename__ = "customer"

    customer_id = Column(Integer, primary_key=True, autoincrement=True)

    customer_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    social_number = Column(String(15), nullable=False)
    birthdate = Column(DateTime(timezone=True), nullable=False)

    customer_type = Column(String(15), nullable=False)
    credit_limit = Column(Numeric(19, 4), nullable=False, default=0.0)

    country_id = Column(
        Integer, ForeignKey("person.countryregion.country_id"), primary_key=True, nullable=False
    )
    state_province_id = Column(
        Integer, ForeignKey("person.stateprovince.state_province_id"), nullable=False
    )

    demographics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    __table_args__ = (
        Index(
            "ix_customer_dem_occupation", cast(demographics["social"]["occupation"].astext, String)
        ),
        Index(
            "ix_customer_dem_buying_contact_name",
            cast(demographics["social"]["buying_contact"]["name"].astext, String),
        ),
        Index("ix_customer_dem_addr_city", cast(demographics["address"]["city"].astext, String)),
        Index(
            "ix_customer_dem_addr_state", cast(demographics["address"]["state_name"].astext, String)
        ),
        Index(
            "ix_customer_dem_addr_country", cast(demographics["address"]["country"].astext, String)
        ),
        Index(
            "ix_customer_dem_metrics_first_purchase",
            cast(demographics["metrics"]["date_of_first_purchase"].astext, String),
        ),
        Index(
            "ix_customer_dem_metrics_last_purchase",
            cast(demographics["metrics"]["date_last_purchase"].astext, String),
        ),
        Index(
            "ix_customer_dem_metrics_total_orders",
            cast(demographics["metrics"]["count_orders"].astext, Integer),
        ),
        Index("ix_customer_demographics_gin", demographics, postgresql_using="gin"),
        {"schema": "person"},
    )
