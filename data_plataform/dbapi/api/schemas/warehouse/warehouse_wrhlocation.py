from enum import Enum
from operator import gt
import uuid
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime

class WarehouseType(str, Enum):
    
    COLD_STORAGE = "cold_storage"
    HOT_STORAGE = "hot_storage"
    DRY = "dry"
    HAZARED = "hazarded"

class TemperatureRange(BaseModel):
    min_temp: float
    max_temp: float
    
    @model_validator(mode='after')
    def validate_temp_range(self):
        if self.max_temp < self.min_temp:
            raise ValueError(f"Maximum temperature ({self.max_temp}) cannot be less than the minimum ({self.min_temp})")
        return self

class LocationCapabilities(BaseModel):
    warehouse_type: WarehouseType = Field(default=WarehouseType.DRY)
    temperature_range: Optional[TemperatureRange] = None
    is_secure_area: bool = False
    
    aisle: Optional[str] = Field(None, description="Aisle")
    shelf: Optional[str] = Field(None, description="Shelf")
    bin:   Optional[str] = Field(None, description="Bin", alias= "bin")
    
    
class GeoLocation(BaseModel):
    address: str
    address_line: str
    warehouse_number: str
    
    country_id: int =  Field(gt=0)
    country_name: str = Field(max_length=100)
    state_province_id: int = Field(gt=0)
    state_province_name: str = Field(max_length=100)
    city_id: int = Field(gt=0)
    city_name: str = Field(max_length=100)
    postal_code: str = Field(max_length=20)
    

class WarehouseLocationSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, 
                              use_enum_values=True)

    warehouselocation_id: int = Field(gt=0)
    warehouse_id: int = Field(gt=0)
    
    name: str = Field(max_length=50)
    
    cost_rate: Decimal = Field(ge=0.0, decimal_places=2)
    availability: Decimal = Field(ge=0.0, decimal_places=2)
    
    capabilities: LocationCapabilities
    geo_location: GeoLocation

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)