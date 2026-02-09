import uuid
from pydantic import BaseModel, Field
from datetime import datetime

class SpecialOfferProduct(BaseModel):
    
    special_offer_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True