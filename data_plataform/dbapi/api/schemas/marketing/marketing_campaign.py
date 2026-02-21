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
    promoted_product_categories: List[str] = Field(default_factory=list)
    
class MarketingCampaign(BaseModel):
    
    campaign_id: int = Field(gt=0)
    campaign_name: str = Field(max_length=100)
    utm_campaign: str = Field(max_length=100)
    
    budget_type: MarketingBudgeType
    total_budget: Decimal = Field(ge=0.0, decimal_places=2)
    
    start_date: datetime
    end_date: Optional[datetime] = None
    
    status: CampaignStatus
    
    settings: CampaignSettings
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    #validação temporal de campanha
    @model_validator(mode='after')
    def validate_dates(self):
        # Só valida se tiver data final
        if self.end_date and self.start_date:
            if self.end_date < self.start_date:
                raise ValueError(f"A data final ({self.end_date}) não pode ser anterior à data inicial.")
        return self
    
    class Config:
        from_attributes = True
        use_enum_values = True