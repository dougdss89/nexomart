from decimal import Decimal
from enum import Enum
import uuid
from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl, EmailStr, ConfigDict, model_validator
from datetime import datetime

class Status(str, Enum):
    
    WAITING_APPROVAL = "waiting_approval"
    APPROVED = "approved"
    REJECTED = "rejected"
    CANCELED = "canceled"
    PENDING = "pending"
    COMPLETED = "completed"
    
class AuditAction(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    APPROVED = "approved"
    REJECTED = "rejected"
    CLOSED = "closed"
    CANCELED = "canceled"
    
class AuditLogEntry(BaseModel):
    
    action_date: datetime = Field(default_factory=datetime.now, alias="date")
    action: AuditAction
    employee_id: str = Field(max_length=100, description= "User loginID")
    comments: Optional[str] = None
    
    created_at: datetime = Field(default_factory=datetime.now)
    
class PurchaseOrderHeader(BaseModel):
    
    model_config = ConfigDict(
        from_attributes=True,
        use_enum_values=True
    )
    
    purchaseorderheader_id: str = Field(max_length=36)
    vendor_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    
    order_date: datetime
    ship_date: datetime
    orderfinished_date: Optional[datetime] = None
    
    # TIP: In PostgreSQL, decimal(19,4) requires 4 decimal places in Pydantic to avoid silent truncation in the database
    sub_total: Decimal = Field(ge=0.0, decimal_places=4)
    tax_amt: Decimal = Field(ge=0.0, decimal_places=4)
    freight: Decimal = Field(ge=0.0, decimal_places=4)
    total_due: Decimal = Field(ge=0.0, decimal_places=4)
    
    status: Status
    audit_trail: List[AuditLogEntry] = Field(default_factory=list)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def validate_purchase_order(self):
        # Date Validation
        if self.ship_date < self.order_date:
            raise ValueError("The ship date (ship_date) cannot be earlier than the order date (order_date).")

        if self.orderfinished_date and self.orderfinished_date < self.order_date:
            raise ValueError("The finish date cannot be earlier than the order date.")

        # Totals Validation
        calculated_total = self.sub_total + self.tax_amt + self.freight
        if abs(self.total_due - calculated_total) > Decimal("0.01"):
            raise ValueError(f"total_due ({self.total_due}) must be the sum of sub_total, tax_amt and freight ({calculated_total}).")

        # Business Rule: A finished order must have a finish date
        if self.status == Status.COMPLETED and not self.orderfinished_date:
            raise ValueError("Orders with status 'completed' must have a finish date (orderfinished_date).")

        return self
