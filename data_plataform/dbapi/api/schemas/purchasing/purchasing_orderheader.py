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
    
    purchaseorderheader_id: str = Field(max_length=100)
    vendor_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    
    order_date: datetime
    ship_date: datetime
    orderfinished_date: Optional[datetime] = None
    
    # DICA: No PostgreSQL, decimal(19,4) exige 4 casas no Pydantic para evitar truncamento silencioso no banco
    subtotal: Decimal = Field(ge=0.0, decimal_places=4)
    tax: Decimal = Field(ge=0.0, decimal_places=4)
    freight: Decimal = Field(ge=0.0, decimal_places=4)
    total_due: Decimal = Field(ge=0.0, decimal_places=4)
    
    status: Status
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def validate_purchase_order(self):
        # Validação de Datas
        if self.ship_date < self.order_date:
            raise ValueError("A data de envio (ship_date) não pode ser anterior à data do pedido (order_date).")
        
        if self.orderfinished_date and self.orderfinished_date < self.order_date:
            raise ValueError("A data de finalização não pode ser anterior à data do pedido.")

        # Validação de Totais
        calculated_total = self.subtotal + self.tax + self.freight
        if abs(self.total_due - calculated_total) > Decimal("0.01"):
            raise ValueError(f"O total_due ({self.total_due}) deve ser a soma de subtotal, tax e freight ({calculated_total}).")

        # Regra de Negócio: Pedido finalizado deve ter data de finalização
        if self.status == Status.COMPLETED and not self.orderfinished_date:
            raise ValueError("Pedidos com status 'completed' devem ter uma data de finalização (orderfinished_date).")

        return self
