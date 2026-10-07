import uuid
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime
from enum import Enum
from typing import Optional, List

class SalesEventType(str, Enum):
    PAYMENT_APPROVED = "payment_approved"
    ITEM_PICKED = "item_picked"
    DELIVERY_APPROVED = "delivery_approved"
    DELIVERY_ATTEMPT_FAILED = "delivery_attempt_failed"
    IN_SEPARATION = "in_separation"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"


class GPSLocation(BaseModel):
    lat: float = Field(..., description="Latitude")
    long: float = Field(..., description="Longitude")

# The Flexible Payload (JSONB)
class SalesEventContext(BaseModel):
    user_id: Optional[int] = Field(None, gt=0, description="Can be null for system events")
    affected_line_ids: List[int] = Field(default_factory=list, description="List of order items")
    reason: Optional[str] = Field(None, max_length=200)
    gps_location: Optional[GPSLocation] = None

# The main table
class SalesOrderEventLogSchema(BaseModel):
    
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
    
    event_id: Optional[int] = Field(None, gt=0, description="None = auto-increment in the database")
    order_id: int = Field(gt=0)

    # COUNTERPOINT: Back to the Top of the Table!
    event_type: SalesEventType
    event_date: datetime = Field(default_factory=datetime.now)

    # The variable context (JSONB)
    event_data: SalesEventContext = Field(default_factory=SalesEventContext)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4) 
    
    @model_validator(mode='after')
    def validate_event_date(self) -> 'SalesOrderEventLogSchema':
        # Now we validate event_date directly from the class root
        if self.event_date > self.created_at:
            raise ValueError("event_date cannot be in the future relative to created_at")
        return self