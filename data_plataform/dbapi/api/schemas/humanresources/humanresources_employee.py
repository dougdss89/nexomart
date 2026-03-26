import uuid
from decimal import Decimal
from typing import List, Union, Dict, Any
from pydantic import BaseModel, Field, model_validator, EmailStr, ConfigDict, Optional
from datetime import date, datetime

class SalesRoleAttributes(BaseModel):
    sales_quota_quaterly: Decimal = Field(ge=0.0, decimal_places=2)
    territory_codes: List[str] = Field(default_factory=list, min_length=1)
    # Comissão dinâmica baseada em Senioridade
    comission_pct: Decimal = Field(ge=0.0, le=100.0, decimal_places=2)
    portfolio_size_limit: Optional[int] = Field(None, gt=0)

class SalesManagerRoleAttributes(BaseModel):
    # Gerentes operam com bônus anual em vez de comissão direta
    annual_bonus_target_pct: Decimal = Field(ge=0.0, le=100.0, decimal_places=2)
    quota_achievement_multiplier: float = Field(ge=0.0)
    managed_territory_codes: List[str] = Field(default_factory=list, min_length=1)

class BuyerRoleAttributes(BaseModel):
    
    managed_categories: List[str] = Field(default_factory=list, min_length=1)
    approval_limit_value: Decimal = Field(ge=0.0, decimal_places=2)
    warehouse_sections: List[str] = Field(default_factory=list, min_length=1)
    supplier_rating_min: float = Field(ge=0.0, le=5.0)
    supplier_rating_max: float = Field(ge=0.0, le=5.0)
    
class FieldMarketingAttributes(BaseModel):
    
    device_id: str = Field(max_length=100)
    survey_zones: List[str] = Field(default_factory=list, min_length=1)
    shift_schedule: str = Field(pattern=r"^\d{2}:\d{2}-\d{2}:\d{2}$", example="08:00-14:00")
    daily_survey_target: int = Field(gt=0)
    

# (Assumindo que SalesRoleAttributes, BuyerRoleAttributes etc. já estão importados)

class EmployeeSchema(BaseModel):
    model_config = ConfigDict(
            from_attributes=True
        )
    
    employee_id: int = Field(gt=0)
    first_name: str = Field(max_length=100) # Ajustei nome para first_name (padrão SQL)
    last_name: str = Field(max_length=100)
    email: EmailStr
    
    nation_id_number: str = Field(pattern=r"^\d{11}$", description="CPF apenas números")
    birth_date: date
    
    hire_date: date
    # CORREÇÃO 1: Data de demissão é opcional (None = Ativo)
    end_date: Optional[date] = None 
    
    login_id: EmailStr
    job_role_id: int = Field(gt=0)
    department_id: int = Field(gt=0)
    
    # CORREÇÃO 2: Manager é opcional (CEO não tem chefe)
    manager_id: Optional[int] = Field(None, gt=0)   
    
    # --- GEOGRAFIA E MOEDA (Expansão Internacional) ---
    country_id: int = Field(gt=0)
    state_province_id: int = Field(gt=0)
    local_currency: str = Field(min_length=3, max_length=3)
    
    # JSONB Polimórfico Expandido
    role_attributes: Union[
        SalesRoleAttributes, 
        SalesManagerRoleAttributes,
        BuyerRoleAttributes, 
        FieldMarketingAttributes, 
        Dict[str, Any]
    ] = Field(default_factory=dict)
    
    current_salary: Decimal = Field(ge=0.0, decimal_places=2)
    
    # --- SCD TIPO 2 (Histórico) ---
    effective_start_date: datetime
    effective_end_date: Optional[datetime] = None 
    is_current: bool 

    # Metadados de Auditoria
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    
    # --- VALIDADOR 1: Lógica de RH (Datas e Chefia) ---
    @model_validator(mode="after")
    def validate_hr_logic(self):
        
        # Só valida demissão se ela existir
        if self.end_date and self.end_date < self.hire_date:
            # Corrigi a mensagem: não pode ser ANTERIOR (antes)
            raise ValueError(f"A data de demissão ({self.end_date}) não pode ser anterior à data de contratação ({self.hire_date}).")
        
        # Validação de auto-chefia
        if self.manager_id and self.manager_id == self.employee_id:
            raise ValueError("Funcionário não pode ser seu próprio gerente (manager_id == employee_id).")
        
        return self
    
    # --- VALIDADOR 2: Consistência do Histórico (SCD2) ---
    # Removi a checagem de max_salary/min_salary pois não existe aqui
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