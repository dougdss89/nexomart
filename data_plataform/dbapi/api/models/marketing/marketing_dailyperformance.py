from api.config.base import AuditMixin, Base  # [FIX] AuditMixin included
from sqlalchemy import Column, Date, ForeignKey, Index, Integer, Numeric, cast, text
from sqlalchemy.dialects.postgresql import JSONB


class MarketingDailyPerformanceModel(Base, AuditMixin):  # [FIX] Inheritance
    """marketing.dailyperformance is RANGE-partitioned by date_ref (2026-08-13, monthly
    partitions marketing.dailyperformance_2026_07..._09 plus a DEFAULT catch-all). Postgres
    requires the partition key in every unique/PK constraint, so the PK became
    (performance_id, date_ref).

    This migration also durably fixed real page-level corruption discovered in the old table
    (blocks 38717-38911, first detected 2026-08-13 11:52 UTC, root cause unresolved) — the
    rewrite-and-swap this migration does (copy readable rows into a new table, drop the old
    one) discards the corrupted physical file; a plain VACUUM under zero_damaged_pages doesn't
    persist because Postgres treats an all-zero page as "mid-extension" and never marks it
    dirty. 2,136 rows were unrecoverable in the corrupted region and are gone.

    No table FKs *into* dailyperformance (leaf fact table), so no dependent model needed updating.
    """

    __tablename__ = "dailyperformance"

    # [FIX] Naming aligned with Schema and addition of the Group level
    performance_id = Column(Integer, primary_key=True, autoincrement=True)
    campaign_id = Column(
        Integer, ForeignKey("marketing.marketingcampaign.campaign_id"), nullable=False
    )
    ad_group_id = Column(
        Integer, ForeignKey("marketing.marketingadgroup.ad_group_id"), nullable=False
    )  # [FIX] Was missing
    ad_id = Column(
        Integer, ForeignKey("marketing.advertise.ad_id"), nullable=False
    )  # [FIX] advertise_id -> ad_id

    # [CRITICAL FIX] Added missing daily timestamp marker (Time-Series DW Requirement)
    date_ref = Column(Date, primary_key=True, nullable=False)

    cost = Column(Numeric(19, 4), nullable=False, default=0.0)
    impressions = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)
    conversions = Column(Integer, nullable=False, default=0)
    conversion_value = Column(
        Numeric(19, 2), nullable=False, default=0.0
    )  # [FIX] revenue -> conversion_value & decimal=2

    metrics = Column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )  # [FIX] platform_metrics -> metrics

    # [FIX] Copy of the Indexes using real keys from the Pydantic JSON
    __table_args__ = (
        Index(
            "ix_performance_metrics_quality_score", cast(metrics["quality_score"].astext, Integer)
        ),
        Index("ix_performance_metrics_likes", cast(metrics["likes"].astext, Integer)),
        Index(
            "ix_performance_metrics_shares", cast(metrics["shares"].astext, Integer)
        ),  # Previously matched incorrectly on quality_share
        {"schema": "marketing"},
    )
