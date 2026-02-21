# schema/purchase_audit.py
import uuid
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from enum import Enum

class AuditAction(str, Enum):
    CREATED = "created"
    MODIFIED = "modified"
    APPROVED = "approved"
    REJECTED = "rejected"
    CLOSED = "closed"

class PurchaseOrderAudit(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
    
    audit_id: int = Field(gt=0)
    
    # A Chave Estrangeira (FK)
    purchaseorderheader_id: str = Field(max_length=100)
    
    action: AuditAction
    
    # Mudamos user para user_id (int) para vincular ao funcionário do RH corretamente
    employee_id: int = Field(gt=0, description="ID do funcionário que executou a ação")
    
    obs: Optional[str] = Field(None, description="Motivo da rejeição ou aprovação")
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)