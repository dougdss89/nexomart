import uuid
from typing import List, Optional
from decimal import Decimal
from pydantic import BaseModel, Field, HttpUrl, EmailStr, ConfigDict, model_validator
from datetime import datetime

class VendorContact(BaseModel):
    
    name: str = Field(max_length=100)
    email: EmailStr
    phone: str = Field(max_length=20)
    
class VendorDetails(BaseModel):
    
    tax_id: str = Field(max_length=20)
    payment_terms: str = Field(max_length=100)
    certifications: List = Field(default_factory=list)
    primary_contact: VendorContact
    average_lead_time_days: int = Field(gt=0)
    on_time_delivery_rate: float = Field(ge=0.0, le=1.0)
    
class Vendor(BaseModel):
    
    model_config = ConfigDict(
            from_attributes=True
        )
    
    vendor_id: int = Field(gt=0)
    business_entity_id: int = Field(gt=0)
    account_number: str = Field(max_length=20)
    
    vendor_name: str = Field(max_length=100)
    credit_rating: Decimal = Field(ge=1, le=5)
    
    vendor_details: VendorDetails
    purchasing_webservice_url: Optional[HttpUrl] = None
    
    # --- SCD TIPO 2 (Histórico) ---
    effective_start_date: datetime
    effective_end_date: Optional[datetime] = None 
    is_current: bool 

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    
    @model_validator(mode='after')
    def validate_vendor_status(self):
        # Regra de Negócio: Se o risco for Alto (5), o fornecedor não pode ser 'is_active'
        # a menos que tenha uma aprovação especial (aqui simplificado)
        if self.credit_rating and self.credit_rating <= 2 and self.is_active:
            # Isso é opcional, depende da regra da sua empresa
            raise ValueError("Fornecedores com risco de crédito menor ou igual a 2 não podem estar ativos automaticamente.")
                        
        return self

    @model_validator(mode="after")
    def validate_scd_consistency(self):
        # Validação temporal do histórico
        if self.effective_end_date and self.effective_end_date < self.effective_start_date:
            raise ValueError("A data final de vigência não pode ser anterior à data inicial.")
        
        # Consistência da flag is_current
        if self.effective_end_date is not None and self.is_current:
            raise ValueError("Um registro histórico finalizado (com data fim) não pode ser marcado como 'is_current=True'.")
        
        if self.effective_end_date is None and self.is_current is False:
            raise ValueError("Um registro vigente (sem data fim) deve ser marcado como 'is_current=True'.")
        
        return self