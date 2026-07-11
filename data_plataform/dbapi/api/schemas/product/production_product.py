import uuid
from pydantic import BaseModel, Field, PositiveFloat, model_validator
from datetime import datetime
from enum import Enum
from typing import Optional, Dict

class ProductDetails(BaseModel):
    
    size: Optional[str] = Field(None, max_length=10, example="G")
    color: Optional[str] = Field(None, max_length=10, example="Black")
    list_price: PositiveFloat = Field(gt=0.0, description = "custo de produção")
    price: PositiveFloat = Field(gt=0.0, description = "preço de venda")
    tax: PositiveFloat = Field(gt=0.0, description = "imposto do produto")
    weight: Optional[float] = Field(None, description="Peso em kg")
    material: Optional[str] = Field(None, example="Algodão/Poliéster")
    brand: Optional[str] = Field(None, example="Nike")
    dimensions: Optional[Dict[str, float]] = Field(
        None, example={"height": 10.5, "width": 20.0, "depth": 5.0}
    )
    sku: Optional[str] = Field(None, description="Stock Keeping Unit")

    class Config:
        extra = "allow" # Permite inserir atributos extras (Data Engineering challenge: schema evolution/flexibility)

class ProductSchema(BaseModel):
    
    product_id:int = Field(gt=0)
    product_name: str = Field(max_length=100)  
    model_id:int    = Field(gt=0)
    category_id:int = Field(gt=0)
    product_details: ProductDetails
    sell_start_date: datetime = Field(default_factory=datetime.now)
    sell_end_date: Optional[datetime] = Field(None, description="NULL = produto ainda em venda")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active:bool = True
    row_guid:uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_sell_dates(self):
        if self.sell_end_date and self.sell_end_date < self.sell_start_date:
            raise ValueError(f"A data de fim de venda ({self.sell_end_date}) não pode ser anterior à data de início ({self.sell_start_date}).")
        return self

    class Config:
        from_attributes = True #pydantic lendo modelos do SQLAlchemy