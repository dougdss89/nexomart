import uuid
from decimal import Decimal
from typing import List, Union, Dict, Any, Optional
from pydantic import BaseModel, Field, model_validator, EmailStr, ConfigDict
from datetime import date, datetime

class SalesRoleAttributes(BaseModel):
    sales_quota_quaterly: Decimal = Field(ge=0.0, decimal_places=2)
    territory_codes: List[str] = Field(default_factory=list, min_length=1)
    # Dynamic commission based on Seniority
    comission_pct: Decimal = Field(ge=0.0, le=100.0, decimal_places=2)
    portfolio_size_limit: Optional[int] = Field(None, gt=0)

class SalesManagerRoleAttributes(BaseModel):
    # Managers operate with an annual bonus instead of direct commission
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
    

# (Assuming SalesRoleAttributes, BuyerRoleAttributes etc. are already imported)

class EmployeeSchema(BaseModel):
    model_config = ConfigDict(
            from_attributes=True
        )
    
    employee_id: int = Field(gt=0)
    first_name: str = Field(max_length=100) # Renamed to first_name (SQL standard)
    last_name: str = Field(max_length=100)
    email: EmailStr

    nation_id_number: str = Field(pattern=r"^\d{11}$", description="CPF, numbers only")
    birth_date: date

    hire_date: date
    # FIX 1: Termination date is optional (None = Active)
    end_date: Optional[date] = None

    login_id: EmailStr
    job_role_id: int = Field(gt=0)
    department_id: int = Field(gt=0)

    # FIX 2: Manager is optional (CEO has no boss)
    manager_id: Optional[int] = Field(None, gt=0)

    # --- GEOGRAPHY AND CURRENCY (International Expansion) ---
    country_id: int = Field(gt=0)
    state_province_id: int = Field(gt=0)
    local_currency: str = Field(min_length=3, max_length=3)
    
    # Expanded Polymorphic JSONB
    role_attributes: Union[
        SalesRoleAttributes, 
        SalesManagerRoleAttributes,
        BuyerRoleAttributes, 
        FieldMarketingAttributes, 
        Dict[str, Any]
    ] = Field(default_factory=dict)
    
    current_salary: Decimal = Field(ge=0.0, decimal_places=2)
    
    # --- SCD TYPE 2 (History) ---
    effective_start_date: datetime
    effective_end_date: Optional[datetime] = None
    is_current: bool

    # Audit Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: Optional[datetime] = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)


    # --- VALIDATOR 1: HR Logic (Dates and Management Chain) ---
    @model_validator(mode="after")
    def validate_hr_logic(self):

        # Only validate termination if it exists
        if self.end_date and self.end_date < self.hire_date:
            # Fixed the message: cannot be BEFORE (earlier than)
            raise ValueError(f"The termination date ({self.end_date}) cannot be earlier than the hire date ({self.hire_date}).")

        # Self-management validation
        if self.manager_id and self.manager_id == self.employee_id:
            raise ValueError("An employee cannot be their own manager (manager_id == employee_id).")

        return self

    # --- VALIDATOR 2: History Consistency (SCD2) ---
    # Removed the max_salary/min_salary check since it doesn't exist here
    @model_validator(mode="after")
    def validate_scd_consistency(self):

        # Temporal validation of the history
        if self.effective_end_date and self.effective_end_date < self.effective_start_date:
            raise ValueError("The effective end date cannot be earlier than the effective start date.")

        # Consistency of the is_current flag
        if self.effective_end_date is not None and self.is_current:
            raise ValueError("A closed historical record (with an end date) cannot be marked as 'is_current=True'.")

        if self.effective_end_date is None and self.is_current is False:
            raise ValueError("A current record (without an end date) must be marked as 'is_current=True'.")

        return self