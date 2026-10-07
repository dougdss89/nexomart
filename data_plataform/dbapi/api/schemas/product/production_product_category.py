import uuid
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum
from typing import Optional

class ProductCategory(str, Enum):
    
    ELECTRONICS = "electronics"
    HOME_APPLIANCE = "home_appliance"
    FURNITURE = "furniture"
    CLOTHING = "clothing"
    FOOTWEAR = "footwear"
    BEAUTY_PERSONAL_CARE = "beauty_personal_care"
    HEALTH_WELLNESS = "health_wellness"
    BOOKS = "books"
    TOYS_GAMES = "toys_games"
    SPORTS_OUTDOORS = "sports_outdoors"
    AUTOMOTIVE = "automotive"
    GROCERY = "grocery"
    PET_SUPPLIES = "pet_supplies" 
    OFFICE_SUPPLIES = "office_supplies"
    GARDEN_TOOLS = "garden_tools"
    MUSICAL_INSTRUMENTS = "musical_instruments"
    JEWELRY_WATCHES = "jewelry_watches"
    HOME_IMPROVEMENT = "home_improvement"
    VIDEO_GAMES = "video_games"
    
class ProductCategorySchema(BaseModel):
    """productcategory predates AuditMixin (see CLAUDE.md) — no updated_at/is_active/row_guid,
    a single created_date column instead of created_at, and its own category_identifier UUID.
    Fields here must mirror api/models/product/productcategory.py 1:1, not the AuditMixin shape
    every other schema uses."""

    category_id: int = Field(gt=0) #garante id > 0
    category_name: ProductCategory
    parent_category_id: Optional[int] = Field(None, description="NULL = categoria raiz")
    category_identifier: uuid.UUID = Field(default_factory=uuid.uuid4)
    created_date: datetime = Field(default_factory=datetime.now)

    class Config:
        use_enum_values = True #Pydantic exporta os valores do enum