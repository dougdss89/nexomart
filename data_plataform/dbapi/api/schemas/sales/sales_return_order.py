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
    inspector_user_id: Optional[int] = Field(None, description="Quem fez a vistoria na doca")
    inspection_date: Optional[datetime] = None
    
    condition: Optional[ItemCondition] = None

    is_restockable: bool = Field(default=False, description="True se o WMS deve dar entrada no saldo")
    
    technical_notes: Optional[str] = Field(None, max_length=500, description="Ex: Tela riscada no canto superior")
    
    evidence_urls: List[HttpUrl] = Field(default_factory=list)

# --- MODELO PRINCIPAL ---
class SalesReturnOrderSchema(BaseModel):
    
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)
    
    return_id: str = Field(..., description="Padrão RO-XXXXXXXXX")
    requestorder_code: str = Field(max_length=100)
    
    salesordernumber: str = Field(..., max_length=20)
    customer_id: int = Field(gt=0)
    
    # Opcional, pois uma compra totalmente online pode não ter loja envolvida
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
        # Valida apenas se a return_date não for nula
        if self.return_date and self.return_date < self.requested_date:
            raise ValueError("A data de retorno não pode ser anterior à data solicitada.")   
        
        # Lógica de negócio: Se a inspeção ocorreu, a data de retorno físico tem que existir
        if self.inspection_data.inspection_date and not self.return_date:
            raise ValueError("Não é possível haver data de inspeção sem a data de retorno físico (return_date).")
            
        return self