import uuid
from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator


class InventoryAttributes(BaseModel):
    lot_number: str | None = Field(None, description="Manufacturing lot")
    manufacturing_date: date | None = None
    expiration_date: date | None = None

    @model_validator(mode="after")
    def validate_dates(self):
        if self.manufacturing_date and self.expiration_date:
            if self.expiration_date < self.manufacturing_date:
                raise ValueError(
                    "The expiration date cannot be earlier than the manufacturing date."
                )
        return self


class ProductInventorySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    inventory_id: int | None = Field(
        None, gt=0, description="None = auto-increment in the database"
    )
    product_id: int = Field(gt=0)
    warehouselocation_id: int = Field(gt=0)

    quantity: int = Field(ge=0)
    bin_code: str | None = Field(None, max_length=10)
    last_restock_date: datetime | None = None

    attributes: InventoryAttributes = Field(default_factory=InventoryAttributes)

    modified_date: datetime = Field(default_factory=datetime.now)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
