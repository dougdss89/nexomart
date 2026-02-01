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
    
class ProductCategorySchema(BaseModel):
    
    category_id: int = Field(gt=0) #garante id > 0
    category_name: ProductCategory
    created_date: datetime
    created_at: datetime = Field(default_factory=datetime.now)
    upadated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
class Config:
    use_enum_values = True #Pydantic exporta os valores do enum