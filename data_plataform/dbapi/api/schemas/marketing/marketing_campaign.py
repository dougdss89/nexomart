from decimal import Decimal
from enum import Enum
from typing import Optional, List
import uuid
from pydantic import BaseModel, Field, model_validator
from datetime import datetime


class MarketingBudgeType(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    YEARLY = "yearly"
    LIFETIME = "lifetime"
    
class CampaignStatus(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    
class CampaignSettings(BaseModel):
    target_audience: Optional[str] = Field(None, max_length=255)
    objective: Optional[str] = Field(None, max_length=100)
    conversion_goal: Optional[str] = Field(None, max_length=100)
    bidding_strategy: Optional[str] = Field(None, max_length=100)
    target_roas: Optional[Decimal] = Field(None, ge=0.0, decimal_places=2)
    target_cpa: Optional[Decimal] = Field(None, ge=0.0, decimal_places=2)
    promoted_product_categories: List[str] = Field(default_factory=list)
    
    # Advanced Platform Enrichments
    location_targeting: List[str] = Field(default_factory=list)
    device_targeting: List[str] = Field(default_factory=list)
    language_targeting: List[str] = Field(default_factory=list)
    placement_inclusions: List[str] = Field(default_factory=list)
    ad_schedule: Optional[str] = Field(None, description="e.g. 'Mon-Fri 09:00-18:00'")
    is_dynamic_creative: bool = Field(default=False)
    
class MarketingCampaign(BaseModel):
    
    campaign_id: int = Field(gt=0)
    channel_id: int = Field(gt=0) # [FIX] Added FK
    campaign_name: str = Field(max_length=100)
    utm_campaign: str = Field(max_length=100)
    
    budget_type: MarketingBudgeType
    total_budget: Decimal = Field(ge=0.0, decimal_places=2)
    
    start_date: datetime
    end_date: Optional[datetime] = None
    
    status: CampaignStatus
    
    mktsettings: CampaignSettings # [FIX] Alignment with the SQLAlchemy Model

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    
    #temporal validation of the campaign
    @model_validator(mode='after')
    def validate_dates(self):
        # Only validate if there is an end date
        if self.end_date and self.start_date:
            if self.end_date < self.start_date:
                raise ValueError(f"The end date ({self.end_date}) cannot be earlier than the start date.")
        return self
    
    class Config:
        from_attributes = True
        use_enum_values = True