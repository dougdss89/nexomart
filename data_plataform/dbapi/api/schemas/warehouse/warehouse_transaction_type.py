import uuid
from typing import Optional
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, model_validator

class ActionEffect(str, Enum):
    IN = "positive"
    OUT = "negative"
    NEUTRAL = "neutral" 

class TransactionTypeSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
    
    transactiontype_id: int = Field(gt=0)
    transaction_code: str = Field(min_length=1, max_length=2)
    transaction_name: str = Field(max_length=50)
    description: Optional[str] = Field(None, max_length=255)
    action_effect: ActionEffect
    
    valid_from: datetime = Field(default_factory=datetime.now)
    valid_to: Optional[datetime] = None
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_scd(self):
        if self.valid_to and self.valid_to < self.valid_from:
            raise ValueError("The valid_to date cannot be earlier than valid_from.")
        return self