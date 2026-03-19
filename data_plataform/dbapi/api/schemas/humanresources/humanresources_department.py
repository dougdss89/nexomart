from datetime import datetime
from enum import Enum
from typing import Optional
import uuid
from pydantic import BaseModel, ConfigDict, Field, model_validator

class HRDepartmentName(str, Enum):
    DOCUMENT_CONTROL = "document control"
    ENGINEERING = "engineering"
    EXECUTIVE = "executive"
    FACILITIES_AND_MAINTENANCE = "facilities and maintenance"
    FINANCE = "finance"
    HUMAN_RESOURCES = "human resources"
    INFORMATION_SERVICES = "information services"
    MARKETING = "marketing"
    PRODUCTION = "production"
    PRODUCTION_CONTROL = "production control"
    PURCHASING = "purchasing"
    QUALITY_ASSURANCE = "quality assurance"
    RESEARCH_AND_DEVELOPMENT = "research and development"
    SALES = "sales"
    SHIPPING_AND_RECEIVING = "shipping and receiving"
    TOOL_DESIGN = "tool design"

class HRGroupName(str, Enum):

    EXECUTIVE_GENERAL_AND_ADMINISTRATION = "executive general and administration"
    INVENTORY_MANAGEMENT = "inventory management"
    MANUFACTURING = "manufacturing"
    QUALITY_ASSURANCE = "quality assurance"
    RESEARCH_AND_DEVELOPMENT = "research and development"
    SALES_AND_MARKETING = "sales and marketing"
    
# Mapeamento estático baseado na imagem enviada
DEPARTMENT_TO_GROUP_MAP = {
    HRDepartmentName.ENGINEERING: HRGroupName.RESEARCH_AND_DEVELOPMENT,
    HRDepartmentName.TOOL_DESIGN: HRGroupName.RESEARCH_AND_DEVELOPMENT,
    HRDepartmentName.RESEARCH_AND_DEVELOPMENT: HRGroupName.RESEARCH_AND_DEVELOPMENT,
    
    HRDepartmentName.SALES: HRGroupName.SALES_AND_MARKETING,
    HRDepartmentName.MARKETING: HRGroupName.SALES_AND_MARKETING,
    
    HRDepartmentName.PURCHASING: HRGroupName.INVENTORY_MANAGEMENT,
    HRDepartmentName.SHIPPING_AND_RECEIVING: HRGroupName.INVENTORY_MANAGEMENT,
    
    HRDepartmentName.PRODUCTION: HRGroupName.MANUFACTURING,
    HRDepartmentName.PRODUCTION_CONTROL: HRGroupName.MANUFACTURING,
    
    HRDepartmentName.HUMAN_RESOURCES: HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION,
    HRDepartmentName.FINANCE: HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION,
    HRDepartmentName.INFORMATION_SERVICES: HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION,
    HRDepartmentName.FACILITIES_AND_MAINTENANCE: HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION,
    HRDepartmentName.EXECUTIVE: HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION,
    
    HRDepartmentName.DOCUMENT_CONTROL: HRGroupName.QUALITY_ASSURANCE,
    HRDepartmentName.QUALITY_ASSURANCE: HRGroupName.QUALITY_ASSURANCE,
}

class HRDepartment(BaseModel):
    
    model_config = ConfigDict(
            from_attributes=True,
            use_enum_values=True
        )
        
    department_id: int = Field(gt=0)
    
    department_name: HRDepartmentName
    group_name: Optional[HRGroupName] = None

    manager_id: Optional[int] = Field(None, gt=0)
    parent_department_id: Optional[int] = Field(None, gt=0)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def set_group_based_on_departament(self):
        
        correct_group = DEPARTMENT_TO_GROUP_MAP.get(self.department_name)
        
        if correct_group:
            self.group_name = correct_group
            
        return self
    
