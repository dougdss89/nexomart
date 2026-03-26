from decimal import Decimal
from enum import Enum
from typing import Optional
import uuid
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime


class OfferCategory(str, Enum):
    RESELLER = "reseller"
    CUSTOMER = "customer"
    
class DiscountType(str, Enum):
    
    SEASONAL = "seasonal"
    VOLUME = "volume"
    DISCONTINUED = "discontinued"
    NEW_PRODUCT = "new_product"
    

class SpecialOffer(BaseModel):
    
    special_offer_id: int = Field(gt=0)
    description: str = Field(max_length=100, description="Descrição da oferta")
    discount_pct: Decimal = Field(ge=0.0, le=100.00, decimal_places=2, description="Porcentagem de desconto")
    category: OfferCategory
    discount_type: DiscountType
    
    min_quantity: int = Field(ge=0, default= 0, description="Quantidade mínima")
    max_quantity: Optional[int] = Field(ge=0, default= 0, description="Quantidade máxima")
    
    start_date: datetime = Field(default_factory=datetime.now)
    end_date: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4) 
    
    
    @model_validator(mode='after')
    def validate_dates(self):
        if self.end_date < self.start_date:
            raise ValueError("A data final (end_date) não pode ser anterior à data inicial (start_date).")
    
        if self.max_quantity is not None and self.max_quantity < self.min_quantity:
            raise ValueError("A quantidade máxima (max_quantity) não pode ser menor que a quantidade mínima (min_quantity).")
        
        return self

    class Config:
        from_attributes = True
        use_enum_values = True
