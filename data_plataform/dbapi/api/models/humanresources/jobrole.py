from sqlalchemy import Column, Integer, Boolean, DateTime, String, Numeric
from sqlalchemy.sql import func
from api.models.base import AuditMixin, Base

class HumanResourcesJobRoleModel(Base, AuditMixin):
    
    __tablename__ = "jobrole"
    __table_args__ = {"schema": "humanresources"}
    
    jobrole_id = Column(Integer, primary_key = True, autoincrement = True)
    jobtitle = Column(String(100), nullable = False)
    
    min_salary = Column(Numeric(19,4), nullable = False, default = 0.0)
    max_salary = Column(Numeric(19,4), nullable = True, default = 0.0)
    
    effective_startdate = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    effective_enddate = Column(DateTime(timezone=True), nullable = True, server_default=func.now())
    
    is_current = Column(Boolean, nullable = False, default = True)
