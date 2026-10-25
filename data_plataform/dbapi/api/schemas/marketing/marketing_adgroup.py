import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, model_validator


class DeviceType(str, Enum):
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"
    TV = "tv"


class AgeRange(BaseModel):
    min_age: int = Field(ge=13, le=100)
    max_age: int = Field(ge=13, le=100)

    @model_validator(mode="after")
    def validate_age_range(self):
        if self.max_age < self.min_age:
            raise ValueError(
                f"Maximum age {self.max_age} cannot be less than the minimum age: {self.min_age}"
            )
        return self


class GeoLocation(BaseModel):
    include: list[str] = Field(default_factory=list, min_lenght=1)
    exclude: list[str] = Field(default_factory=list)


class MarketingTargetCriteria(BaseModel):
    geo_location: GeoLocation | None = None
    age_range: AgeRange | None = None

    interests: list = Field(default_factory=list, example=["techonology", "fashion", "sports"])

    keywords: list[str] = Field(default_factory=list, example=["buy iphone", "iphone deal"])

    device_types: list[DeviceType]

    remarketing_list_ids: list[str] = Field(default_factory=list)


class MarketingAdGroup(BaseModel):
    ad_group_id: int = Field(gt=0)
    campaign_id: int = Field(gt=0)
    ad_group_name: str = Field(max_length=100)
    external_id: str | None = Field(None, max_length=100)

    targeting_criteria: MarketingTargetCriteria

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True
        use_enum_values = True
