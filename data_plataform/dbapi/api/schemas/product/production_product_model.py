import uuid
from pydantic import BaseModel, Field, field_validator
from datetime import datetime
from enum import Enum
from typing import Optional

class ProductModel(str, Enum):
    
    SMARTPHONE = "smartphone"
    LAPTOP_COMPUTING = "laptop_computing"
    WEARABLE_TECH = "wearable_tech"
    KITCHEN_APPLIANCE = "kitchen_appliance"
    LARGE_APPLIANCE = "large_appliance"
    HOME_OFFICE_FURNITURE = "home_office_furniture"
    LIVING_ROOM_FURNITURE = "living_room_furniture"
    MENS_APPAREL = "mens_apparel"
    WOMENS_APPAREL = "womens_apparel"
    ATHLETIC_FOOTWEAR = "athletic_footwear"
    CASUAL_FOOTWEAR = "casual_footwear"
    SKIN_CARE = "skin_care"
    DIETARY_SUPPLEMENTS = "dietary_supplements"
    EDUCATIONAL_TOYS = "educational_toys"
    OUTDOOR_EQUIPMENT = "outdoor_equipment"
    
class ProductModelSchema(BaseModel):
        
    product_model_id:int = Field(gt=0)
    model_name: ProductModel
    created_date: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active:bool = True
    row_guid:uuid.UUID = Field(default_factory= uuid.uuid4)
    
    class Config:
        use_enum_values = True #Pydantic exporta os valores do enum