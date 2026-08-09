import uuid
from decimal import Decimal
from pydantic import BaseModel, Field, ConfigDict, model_validator, HttpUrl
from datetime import datetime
from enum import Enum
from typing import Optional, List

# --- ENUMS ---
class ReturnOrderReason(str, Enum):
    DEFECT = "defect"
    REGRET = "regret"
    CUSTOMER_REQUEST = "customer_request"
    SELLER_REQUEST = "seller_request"
    OTHER = "other"
    
class ReturnRequestStatus(str, Enum):
    PENDING = "pending"
    IN_INSPECTION = "in_inspection" 
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    REFUNDED = "refunded" 

class ItemCondition(str, Enum):
    SEALED = "sealed"                     
    OPENED_INTACT = "opened_intact"       
    DAMAGED_PACKAGING = "damaged_package" 
    DAMAGED_PRODUCT = "damaged_product"   
    USED_OR_DIRTY = "used_or_dirty"      

class InspectionData(BaseModel):
    inspector_user_id: Optional[int] = Field(None, description="Who performed the dock inspection")
    inspection_date: Optional[datetime] = None

    condition: Optional[ItemCondition] = None

    is_restockable: bool = Field(default=False, description="True if the WMS should add it back to stock")

    technical_notes: Optional[str] = Field(None, max_length=500, description="E.g.: Screen scratched in the top corner")
    
    evidence_urls: List[HttpUrl] = Field(default_factory=list)

# --- MODELO PRINCIPAL ---
class SalesReturnOrderSchema(BaseModel):
    
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
    
    return_id: str = Field(..., description="Pattern RO-XXXXXXXXX")
    requestorder_code: str = Field(max_length=100)
    
    salesordernumber: str = Field(..., max_length=20)
    customer_id: int = Field(gt=0)
    
    # Optional, since a fully online purchase may not involve a store
    store_id: Optional[int] = Field(None, gt=0) 
    
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)
    
    return_reason: ReturnOrderReason
    return_status: ReturnRequestStatus
    
    refund_amount: Decimal = Field(ge=0.0, decimal_places=4)
    requested_date: datetime = Field(default_factory=datetime.now)
    return_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    
    inspection_data: InspectionData = Field(default_factory=InspectionData)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def validate_dates(self):
        # Only validates if return_date is not null
        if self.return_date and self.return_date < self.requested_date:
            raise ValueError("The return date cannot be earlier than the requested date.")

        # Business logic: If the inspection occurred, the physical return date must exist
        if self.inspection_data.inspection_date and not self.return_date:
            raise ValueError("There cannot be an inspection date without a physical return date (return_date).")
            
        return self