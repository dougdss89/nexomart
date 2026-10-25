import uuid
from datetime import datetime

from pydantic import BaseModel, Field, PositiveFloat, model_validator


class ProductDetails(BaseModel):
    size: str | None = Field(None, max_length=10, example="G")
    color: str | None = Field(None, max_length=10, example="Black")
    list_price: PositiveFloat = Field(gt=0.0, description="production cost")
    # price/tax use ge=0.0, not PositiveFloat — dbapi/api/samples/product.py intentionally
    # zeroes both out as chaos engineering (zeroed promo price, missing tax); this contract
    # only guarantees "not negative", not "not a chaos value". See CLAUDE.md — Chaos Engineering.
    price: float = Field(ge=0.0, description="sale price")
    tax: float = Field(ge=0.0, description="product tax")
    weight: float | None = Field(None, description="Weight in kg")
    material: str | None = Field(None, example="Cotton/Polyester")
    brand: str | None = Field(None, example="Nike")
    dimensions: dict[str, float] | None = Field(
        None, example={"height": 10.5, "width": 20.0, "depth": 5.0}
    )
    sku: str | None = Field(None, description="Stock Keeping Unit")

    class Config:
        extra = "allow"  # Allows extra attributes to be inserted (Data Engineering challenge: schema evolution/flexibility)


class ProductSchema(BaseModel):
    product_id: int = Field(gt=0)
    product_name: str = Field(max_length=100)
    model_id: int = Field(gt=0)
    category_id: int = Field(gt=0)
    productdetails: ProductDetails
    sell_start_date: datetime = Field(default_factory=datetime.now)
    sell_end_date: datetime | None = Field(None, description="NULL = product still on sale")
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_sell_dates(self):
        if self.sell_end_date and self.sell_end_date < self.sell_start_date:
            raise ValueError(
                f"The sell end date ({self.sell_end_date}) cannot be earlier than the sell start date ({self.sell_start_date})."
            )
        return self

    class Config:
        from_attributes = True  # pydantic reading SQLAlchemy models
