import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator


class SalesRoleAttributes(BaseModel):
    sales_quota_quaterly: Decimal = Field(ge=0.0, decimal_places=2)
    territory_codes: list[str] = Field(default_factory=list, min_length=1)
    # Dynamic commission based on Seniority
    comission_pct: Decimal = Field(ge=0.0, le=100.0, decimal_places=2)
    portfolio_size_limit: int | None = Field(None, gt=0)


class SalesManagerRoleAttributes(BaseModel):
    # Managers operate with an annual bonus instead of direct commission
    annual_bonus_target_pct: Decimal = Field(ge=0.0, le=100.0, decimal_places=2)
    quota_achievement_multiplier: float = Field(ge=0.0)
    managed_territory_codes: list[str] = Field(default_factory=list, min_length=1)


class BuyerRoleAttributes(BaseModel):
    managed_categories: list[str] = Field(default_factory=list, min_length=1)
    approval_limit_value: Decimal = Field(ge=0.0, decimal_places=2)
    warehouse_sections: list[str] = Field(default_factory=list, min_length=1)
    supplier_rating_min: float = Field(ge=0.0, le=5.0)
    supplier_rating_max: float = Field(ge=0.0, le=5.0)


class FieldMarketingAttributes(BaseModel):
    device_id: str = Field(max_length=100)
    survey_zones: list[str] = Field(default_factory=list, min_length=1)
    shift_schedule: str = Field(pattern=r"^\d{2}:\d{2}-\d{2}:\d{2}$", example="08:00-14:00")
    daily_survey_target: int = Field(gt=0)


# (Assuming SalesRoleAttributes, BuyerRoleAttributes etc. are already imported)


class EmployeeSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    employee_id: int = Field(gt=0)
    first_name: str = Field(max_length=100)  # Renamed to first_name (SQL standard)
    last_name: str = Field(max_length=100)
    email: EmailStr

    nationalidnumber: str = Field(pattern=r"^\d{11}$", description="CPF, numbers only")
    birthdate: date

    hire_date: date
    # FIX 1: Termination date is optional (None = Active)
    enddate: date | None = None

    loginid: str = Field(max_length=50)
    jobrole_id: int = Field(gt=0)
    department_id: int = Field(gt=0)

    # FIX 2: Manager is optional (CEO has no boss)
    manager_id: int | None = Field(None, gt=0)

    # --- GEOGRAPHY AND CURRENCY (International Expansion) ---
    country_id: int = Field(gt=0)
    state_province_id: int = Field(gt=0)
    local_currency: str = Field("BRL", min_length=3, max_length=3)

    # Expanded Polymorphic JSONB
    role_attributes: (
        SalesRoleAttributes
        | SalesManagerRoleAttributes
        | BuyerRoleAttributes
        | FieldMarketingAttributes
        | dict[str, Any]
    ) = Field(default_factory=dict)

    current_salary: Decimal = Field(ge=0.0, decimal_places=2)

    # --- SCD TYPE 2 (History) ---
    effective_startdate: datetime = Field(default_factory=datetime.now)
    effective_enddate: datetime | None = None
    is_current: bool

    # Audit Metadata
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime | None = None
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- VALIDATOR 1: HR Logic (Dates and Management Chain) ---
    @model_validator(mode="after")
    def validate_hr_logic(self):
        # Only validate termination if it exists
        if self.enddate and self.enddate < self.hire_date:
            # Fixed the message: cannot be BEFORE (earlier than)
            raise ValueError(
                f"The termination date ({self.enddate}) cannot be earlier than the hire date ({self.hire_date})."
            )

        # Self-management validation
        if self.manager_id and self.manager_id == self.employee_id:
            raise ValueError("An employee cannot be their own manager (manager_id == employee_id).")

        return self

    # --- VALIDATOR 2: History Consistency (SCD2) ---
    # Removed the max_salary/min_salary check since it doesn't exist here
    @model_validator(mode="after")
    def validate_scd_consistency(self):
        # Temporal validation of the history
        if self.effective_enddate and self.effective_enddate < self.effective_startdate:
            raise ValueError(
                "The effective end date cannot be earlier than the effective start date."
            )

        # Consistency of the is_current flag
        if self.effective_enddate is not None and self.is_current:
            raise ValueError(
                "A closed historical record (with an end date) cannot be marked as 'is_current=True'."
            )

        if self.effective_enddate is None and self.is_current is False:
            raise ValueError(
                "A current record (without an end date) must be marked as 'is_current=True'."
            )

        return self
