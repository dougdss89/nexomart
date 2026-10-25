import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class HRJobRoleHistory(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    jobrole_id: int = Field(gt=0)
    job_title: str = Field(max_length=100)

    min_salary: Decimal = Field(ge=0.0, decimal_places=2)
    max_salary: Decimal = Field(ge=0.0, decimal_places=2)
    effective_start_date: datetime
    effective_end_date: datetime | None = None  # Null means it is the current record
    is_current: bool  # Flag to make queries easier

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def max_salary_greater_than_min(self):
        if self.max_salary < self.min_salary:
            raise ValueError("The maximum salary cannot be less than the minimum salary.")

        if self.effective_end_date and self.effective_end_date < self.effective_start_date:
            raise ValueError("The end date cannot be earlier than the start date.")

        if self.effective_end_date is not None and self.is_current:
            raise ValueError("A closed historical record cannot be marked as 'is_current=True'.")

        if self.effective_end_date is None and self.is_current is False:
            raise ValueError("A record without an end date must be marked as 'is_current=True'.")

        return self
