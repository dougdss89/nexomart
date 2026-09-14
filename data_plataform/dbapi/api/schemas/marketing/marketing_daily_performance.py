from datetime import date, datetime
from decimal import Decimal
from typing import Optional
import uuid
from pydantic import BaseModel, Field

class MarketingMetrics(BaseModel):
    # --- VIDEO (Retention Funnel) ---
    video_views: int = Field(default=0, description="Total views (e.g. 3s or 2s)")
    video_views_25: int = Field(default=0)
    video_views_50: int = Field(default=0)
    video_views_75: int = Field(default=0) # The quartile that was missing
    video_views_100: int = Field(default=0)

    # --- SOCIAL ENGAGEMENT ---
    likes: int = Field(default=0)
    comments: int = Field(default=0)
    shares: int = Field(default=0)
    saves: int = Field(default=0, description="Saves (common on Insta/TikTok)")
    clicks_all: int = Field(default=0, description="Total clicks (includes profile clicks, not just link clicks)")

    # --- CONVERSION FUNNEL (Granularity) ---
    adds_to_cart: int = Field(default=0)
    checkouts_initiated: int = Field(default=0)
    leads: int = Field(default=0)
    app_installs: int = Field(default=0)

    # --- COMPETITIVENESS (Google Ads) ---
    # We use Float (0.0 to 1.0 or percentage)
    search_impression_share: Optional[float] = Field(None)
    search_lost_is_budget: Optional[float] = Field(None)
    search_lost_is_rank: Optional[float] = Field(None)

    # --- QUALITY METRICS ---
    quality_score: Optional[int] = Field(None, ge=1, le=10)
    frequency: float = Field(default=1.0)
    reach: int = Field(default=0, description="Unique daily reach")

    created_at: datetime = Field(default_factory=datetime.now)

class MarketingDailyPerformance(BaseModel):
    
    performance_id: int = Field(gt=0)
    campaign_id: int = Field(gt=0)
    ad_id: int = Field(gt=0)
    
    # Added ad_group_id since it is the standard intermediate level (Hierarchy: Campaign > Group > Ad)
    ad_group_id: int = Field(gt=0)

    # Use Python's standard date to guarantee ISO serialization (YYYY-MM-DD)
    date_ref: date = Field(..., description="Performance reference date")

    # --- NAME FIX (Snake Case) ---
    # In the database, columns with uppercase letters (Cost) require quotes ("Cost").
    # The standard in Data Engineering is always lowercase (snake_case).

    cost: Decimal = Field(ge=0.0, decimal_places=4) # 4 decimal places for low CPMs
    impressions: int = Field(ge=0)
    clicks: int = Field(ge=0)
    conversions: int = Field(ge=0) # 'Conv' is too abbreviated, prefer explicit naming
    conversion_value: Decimal = Field(ge=0.0, decimal_places=2)

    # JSONB field in Postgres
    metrics: MarketingMetrics = Field(default_factory=MarketingMetrics)
        
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True