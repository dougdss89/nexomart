# schema/purchase_audit.py
import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class AuditAction(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    APPROVED = "approved"
    REJECTED = "rejected"
    CLOSED = "closed"


class PurchaseOrderAudit(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)

    audit_id: int = Field(gt=0)

    # The Foreign Key (FK)
    purchaseorderheader_id: str = Field(max_length=100)

    action: AuditAction

    # We changed user to user_id (int) to correctly link to the HR employee
    employee_id: int = Field(gt=0, description="ID of the employee who performed the action")

    obs: str | None = Field(None, description="Reason for rejection or approval")

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
