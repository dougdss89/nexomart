from enum import Enum
import uuid
from pydantic import BaseModel, Field
from datetime import datetime


class ChannelName(str, Enum):
    PAID_SEARCH = "paid_search"
    SOCIAL_MEDIA = "social_media"
    EMAIL = "email"
    DIRECT = "direct"
    AFFILIATE = "affiliate"
    ORGANIC = "organic"
    OTHER = "other"
    
class PlatformName(str, Enum):
    # Corrected from 'Plataform' to 'Platform' to match the table
    GOOGLE_ADS = "google_ads"
    META_ADS = "meta_ads"
    TIKTOK = "tiktok"
    LINKEDIN_ADS = "linkedin_ads"
    MAILCHIMP = "mailchimp"
    GA4 = "google_analytics_4"
    NONE = "none"
    
class MarketingChannel(BaseModel):
    channel_id: int = Field(gt=0)
    
    channel_name: ChannelName
    platform_name: PlatformName
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True
        use_enum_values = True