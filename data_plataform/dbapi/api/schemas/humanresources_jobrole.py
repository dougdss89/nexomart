from datetime import datetime
from decimal import Decimal
from typing import Optional, ConfigDict
import uuid
from pydantic import BaseModel, Field, model_validator


class HRJobRoleHistory(BaseModel):
    
    model_config = ConfigDict(
            from_attributes=True
        )
    
    jobrole_id: int = Field(gt=0)
    job_title: str = Field(max_length=100)
    
    min_salary: Decimal = Field(ge=0.0, decimal_places=2)
    max_salary: Decimal = Field(ge=0.0, decimal_places=2) 
    effective_start_date: datetime
    effective_end_date: Optional[datetime] = None # Null significa que é o registro atual
    is_current: bool # Flag para facilitar queries
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def max_salary_greater_than_min(self):
        if self.max_salary < self.min_salary:
            raise ValueError("O salário máximo não pode ser menor que o salário mínimo.")
        
        if self.effective_end_date and self.effective_end_date < self.effective_start_date:
            raise ValueError("A data final não pode ser anterior à data inicial.")
        
        if self.effective_end_date is not None and self.is_current:
            raise ValueError("Um registro histórico finalizado não pode ser marcado como 'is_current=True'.")
        
        if self.effective_end_date is None and self.is_current is False:
            raise ValueError("Um registro sem data final deve ser marcado como 'is_current=True'.")
        
        return self
    
