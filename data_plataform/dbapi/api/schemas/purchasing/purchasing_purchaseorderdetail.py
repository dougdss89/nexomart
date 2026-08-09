import uuid
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime

class PurchaseOrderDetailSchema(BaseModel):
    
    model_config = ConfigDict(from_attributes=True)
    
    # --- KEYS ---
    purchaseorderdetail_id: str = Field(max_length=100)
    purchaseorderheader_id: str = Field(max_length=100, description="FK to PurchaseOrderHeader")
    product_id: int = Field(gt=0, description="FK to the Product/Material")

    # --- DATES ---
    due_date: datetime = Field(..., description="Promised date for THIS item")
    last_receipt_date: Optional[datetime] = Field(None, description="Date of the last actual delivery")

    # --- QUANTITIES AND VALUES ---
    order_qty: int = Field(gt=0, description="Initially ordered quantity")

    # Kept at 4 decimal places for financial consistency with the Header
    unit_price: Decimal = Field(ge=0.0, decimal_places=4)

    # Receipts (Partial or Total)
    received_qty: int = Field(default=0, ge=0, description="Quantity that has already arrived at the dock")
    rejected_qty: int = Field(default=0, ge=0, description="Quantity returned by Quality Control")
    stocked_qty: int = Field(default=0, ge=0, description="Quantity that went into stock")

    # Helper flag (Optional on input, since it can be calculated)
    is_fully_received: bool = Field(default=False)

    # --- AUDIT ---
    # Note: The original diagram had 'modified_date' and 'updated_at'.
    # In Data Engineering, we use only 'updated_at' to avoid redundancy.
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- STOCK LOGIC VALIDATIONS ---

    @model_validator(mode="after")
    def validate_receipt_logic(self):

        # Rule 1: Physical Logic (Cannot reject more than what arrived)
        if self.rejected_qty > self.received_qty:
            raise ValueError(f"The rejected quantity ({self.rejected_qty}) cannot be greater than the received quantity ({self.received_qty}).")

        # Rule 2: Temporal Traceability
        if self.received_qty > 0 and self.last_receipt_date is None:
            raise ValueError("If a receipt occurred (received_qty > 0), the last delivery date (last_receipt_date) must be provided.")

        # Rule 3: Line Closing Consistency
        # The "accepted" quantity (that goes into actual stock) is what arrived minus what was damaged/failed
        accepted_qty = self.received_qty - self.rejected_qty

        if self.is_fully_received and accepted_qty < self.order_qty:
            # Protects against prematurely closing an order that still has pending items
            raise ValueError(
                f"Item cannot be marked as 'is_fully_received=True'. "
                f"Accepted quantity ({accepted_qty}) is less than the ordered quantity ({self.order_qty})."
            )

        # Rule 4: Stock Consistency
        if self.stocked_qty > self.received_qty:
            raise ValueError(f"The stocked quantity ({self.stocked_qty}) cannot be greater than the received quantity ({self.received_qty}).")

        if self.stocked_qty > self.order_qty:
            raise ValueError(f"The stocked quantity ({self.stocked_qty}) cannot be greater than the ordered quantity ({self.order_qty}).")

        # Rule 5: Stock Consistency
        if self.stocked_qty + self.rejected_qty > self.received_qty:
            raise ValueError(f"The stocked quantity ({self.stocked_qty}) + the rejected quantity ({self.rejected_qty}) cannot be greater than the received quantity ({self.received_qty}).")

        if self.stocked_qty + self.rejected_qty > self.order_qty:
            raise ValueError(f"The stocked quantity ({self.stocked_qty}) + the rejected quantity ({self.rejected_qty}) cannot be greater than the ordered quantity ({self.order_qty}).")

        return self