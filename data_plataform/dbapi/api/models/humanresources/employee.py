from sqlalchemy import Column, Integer, DateTime, String, Index, text, Foreign_key, Numeric, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.models.base import Base, AuditMixin

class HumanResourcesJobRoleModel(Base, AuditMixin):
    
    __tablename__ = "employee"
    __table_args__ = (
        Index('ix_role_attributes', 'role_attributes', postrgresql_using = "gin"),
        {"schema": "humanresources"}
    )
    
    employee_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    nationalidnumber = Column(String(50), nullable=False)
    loginid = Column(String(50), nullable=False)
    
    jobrole_id = Column(Integer, Foreign_key('humanresources.jobrole.jobrole_id'), nullable=False, )
    department_id = Column(Integer, Foreign_key('humanresources.department.department_id'), nullable=False)
    manager_id = Column(Integer, Foreign_key('humanresources.employee.employee_id'), nullable=False) 
    
    hire_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    enddate = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    role_attributes = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    current_salary = Column(Numeric(19,4), nullable=False, default=0.0)
    effective_startdate = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    effective_enddate = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    is_current = Column(Boolean, nullable=False, default=True)
    