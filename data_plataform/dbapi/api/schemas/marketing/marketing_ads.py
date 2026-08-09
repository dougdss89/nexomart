
from enum import Enum
from typing import Optional, List
import uuid
from pydantic import BaseModel, Field, AnyUrl, HttpUrl
from datetime import  datetime

class MarketingStatus(str, Enum):
    
    PLANNED = "planned"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    
class MarketingMediaType(str, Enum):
    
    VIDEO = "video"
    IMAGE = "image"
    CAROUSEL = "carousel"
    TEXT = "text"
    
class MediaAsset(BaseModel):
    # HttpUrl validates that it is a real link (starts with http/https)
    # AnyUrl accepts s3://, gs://, etc.
    url: AnyUrl = Field(description="S3 or CDN URL")

    # Optional, since an image has no duration
    duration_seconds: Optional[int] = Field(None, ge=0, description="Duration in seconds (for videos)")
    file_size_mb: Optional[float] = Field(None, ge=0, description="File size in MB")
    width: Optional[int] = Field(None, description="Width in pixels")
    height: Optional[int] = Field(None, description="Height in pixels")

class MarketingCreativeData(BaseModel):

    headline: str = Field(max_length=150)
    description: str = Field(max_length=200)

    # Real URL validation for the ad destination
    final_url: HttpUrl

    media_type: MarketingMediaType

    # THIS IS THE TYPING FIX:
    # Instead of List[Dict], we use List[MediaAsset]
    media_assets: List[MediaAsset] = Field(default_factory=list)
    

class MarketingAd(BaseModel):
    
    ad_id: int = Field(gt=0)
    ad_group_id: int = Field(gt=0)
    ad_name: str = Field(max_length=150)
    external_id: str = Field(max_length=20)
    
    utm_content: Optional[str] = Field(max_length=100)
    utm_medium:  Optional[str] = Field(max_length=100)
    
    ad_status: MarketingStatus
    
    creative_data: MarketingCreativeData
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
        
    class Config:
        from_attributes = True
        use_enum_values = True