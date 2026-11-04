import uuid
from datetime import datetime
from enum import Enum

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


# Organizational structure: each group lists the departments that make it up.
# DEPARTMENT_TO_GROUP_MAP is derived from here to guarantee a single source of truth.
GROUP_TO_DEPARTMENTS: dict[HRGroupName, list[HRDepartmentName]] = {
    HRGroupName.RESEARCH_AND_DEVELOPMENT: [
        HRDepartmentName.ENGINEERING,
        HRDepartmentName.TOOL_DESIGN,
        HRDepartmentName.RESEARCH_AND_DEVELOPMENT,
    ],
    HRGroupName.SALES_AND_MARKETING: [
        HRDepartmentName.SALES,
        HRDepartmentName.MARKETING,
    ],
    HRGroupName.INVENTORY_MANAGEMENT: [
        HRDepartmentName.PURCHASING,
        HRDepartmentName.SHIPPING_AND_RECEIVING,
    ],
    HRGroupName.MANUFACTURING: [
        HRDepartmentName.PRODUCTION,
        HRDepartmentName.PRODUCTION_CONTROL,
    ],
    HRGroupName.EXECUTIVE_GENERAL_AND_ADMINISTRATION: [
        HRDepartmentName.HUMAN_RESOURCES,
        HRDepartmentName.FINANCE,
        HRDepartmentName.INFORMATION_SERVICES,
        HRDepartmentName.FACILITIES_AND_MAINTENANCE,
        HRDepartmentName.EXECUTIVE,
    ],
    HRGroupName.QUALITY_ASSURANCE: [
        HRDepartmentName.DOCUMENT_CONTROL,
        HRDepartmentName.QUALITY_ASSURANCE,
    ],
}

DEPARTMENT_TO_GROUP_MAP: dict[HRDepartmentName, HRGroupName] = {
    department: group
    for group, departments in GROUP_TO_DEPARTMENTS.items()
    for department in departments
}


class HRDepartment(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)

    department_id: int = Field(gt=0)

    department_name: HRDepartmentName
    group_name: HRGroupName | None = None

    manager_id: int | None = Field(None, gt=0)
    parent_department_id: int | None = Field(None, gt=0)

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def set_group_based_on_departament(self):
        correct_group = DEPARTMENT_TO_GROUP_MAP.get(self.department_name)

        if correct_group is None:
            raise ValueError(
                f"Department '{self.department_name}' has no group mapped in GROUP_TO_DEPARTMENTS."
            )

        self.group_name = correct_group

        return self
