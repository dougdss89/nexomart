from api.schemas.humanresources.humanresources_department import (
    HRDepartmentName,
    HRGroupName
)

ORGANOGRAMA = [
    # --- RAIZ (C-Level) ---
    (HRDepartmentName.EXECUTIVE, None),
    
    # --- DIRETORIAS (Respondem ao Executive) ---
    (HRDepartmentName.ENGINEERING, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.SALES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.FINANCE, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.HUMAN_RESOURCES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.INFORMATION_SERVICES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.PRODUCTION, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.PURCHASING, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.FACILITIES_AND_MAINTENANCE, HRDepartmentName.EXECUTIVE),
    
    # --- GERÊNCIAS (Respondem às Diretorias) ---
    (HRDepartmentName.MARKETING, HRDepartmentName.SALES),
    (HRDepartmentName.TOOL_DESIGN, HRDepartmentName.ENGINEERING),
    (HRDepartmentName.RESEARCH_AND_DEVELOPMENT, HRDepartmentName.ENGINEERING),
    (HRDepartmentName.PRODUCTION_CONTROL, HRDepartmentName.PRODUCTION),
    (HRDepartmentName.QUALITY_ASSURANCE, HRDepartmentName.PRODUCTION),
    (HRDepartmentName.SHIPPING_AND_RECEIVING, HRDepartmentName.PURCHASING),
    
    # --- COORDENAÇÕES (Respondem às Gerências) ---
    (HRDepartmentName.DOCUMENT_CONTROL, HRDepartmentName.QUALITY_ASSURANCE),
]