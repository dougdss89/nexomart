import uuid
from pydantic import BaseModel, Field, PositiveFloat
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
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active:bool = True
    row_guid:uuid.UUID = Field(default_factory=uuid.uuid4)
    
    class Config:
        from_attributes = True #pydantic lendo modelos do SQLAlchemy