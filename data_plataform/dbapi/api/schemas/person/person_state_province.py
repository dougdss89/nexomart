import uuid
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

class StateProvince(BaseModel):
    
    state_province_id: int = Field(gt=0)
    state_province_name: str = Field(max_length=100)
    country_id: int = Field(gt=0, description="Country ID captured from the country table")
    state_province_code: str = Field(min_length=2, max_length=5)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    #VALIDATIONS
    
    @field_validator('state_province_code')
    @classmethod
    def validate_state_code(cls, state: str) -> str:
        return state.upper()
    
    @field_validator('state_province_name')
    @classmethod
    def validate_state_name(cls, state_name: str) -> str:
        return state_name.title()
    

    class Config:
        from_attributes = True
