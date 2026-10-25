from api.schemas.humanresources.humanresources_department import HRDepartmentName

ORG_CHART = [
    # --- ROOT (C-Level) ---
    (HRDepartmentName.EXECUTIVE, None),
    # --- DIRECTORATES (Report to Executive) ---
    (HRDepartmentName.ENGINEERING, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.SALES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.FINANCE, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.HUMAN_RESOURCES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.INFORMATION_SERVICES, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.PRODUCTION, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.PURCHASING, HRDepartmentName.EXECUTIVE),
    (HRDepartmentName.FACILITIES_AND_MAINTENANCE, HRDepartmentName.EXECUTIVE),
    # --- MANAGEMENT (Report to Directorates) ---
    (HRDepartmentName.MARKETING, HRDepartmentName.SALES),
    (HRDepartmentName.TOOL_DESIGN, HRDepartmentName.ENGINEERING),
    (HRDepartmentName.RESEARCH_AND_DEVELOPMENT, HRDepartmentName.ENGINEERING),
    (HRDepartmentName.PRODUCTION_CONTROL, HRDepartmentName.PRODUCTION),
    (HRDepartmentName.QUALITY_ASSURANCE, HRDepartmentName.PRODUCTION),
    (HRDepartmentName.SHIPPING_AND_RECEIVING, HRDepartmentName.PURCHASING),
    # --- COORDINATION (Report to Management) ---
    (HRDepartmentName.DOCUMENT_CONTROL, HRDepartmentName.QUALITY_ASSURANCE),
]
