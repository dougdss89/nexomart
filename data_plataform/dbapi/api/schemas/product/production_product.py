import uuid
from pydantic import BaseModel, Field, PositiveFloat, model_validator
from datetime import datetime
from enum import Enum
from typing import Optional, Dict

class ProductDetails(BaseModel):
    
    size: Optional[str] = Field(None, max_length=10, example="G")
    color: Optional[str] = Field(None, max_length=10, example="Black")
    list_price: PositiveFloat = Field(gt=0.0, description = "production cost")
    price: PositiveFloat = Field(gt=0.0, description = "sale price")
    tax: PositiveFloat = Field(gt=0.0, description = "product tax")
    weight: Optional[float] = Field(None, description="Weight in kg")
    material: Optional[str] = Field(None, example="Cotton/Polyester")
    brand: Optional[str] = Field(None, example="Nike")
    dimensions: Optional[Dict[str, float]] = Field(
        None, example={"height": 10.5, "width": 20.0, "depth": 5.0}
    )
    sku: Optional[str] = Field(None, description="Stock Keeping Unit")

    class Config:
        extra = "allow" # Allows extra attributes to be inserted (Data Engineering challenge: schema evolution/flexibility)

class ProductSchema(BaseModel):
    
    product_id:int = Field(gt=0)
    product_name: str = Field(max_length=100)  
    model_id:int    = Field(gt=0)
    category_id:int = Field(gt=0)
    product_details: ProductDetails
    sell_start_date: datetime = Field(default_factory=datetime.now)
    sell_end_date: Optional[datetime] = Field(None, description="NULL = product still on sale")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active:bool = True
    row_guid:uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_sell_dates(self):
        if self.sell_end_date and self.sell_end_date < self.sell_start_date:
            raise ValueError(f"The sell end date ({self.sell_end_date}) cannot be earlier than the sell start date ({self.sell_start_date}).")
        return self

    class Config:
        from_attributes = True #pydantic reading SQLAlchemy models