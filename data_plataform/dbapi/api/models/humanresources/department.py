from sqlalchemy import Column, Integer, String, ForeignKey
from api.config.base import AuditMixin, Base
from sqlalchemy.orm import relationship

class HumanResourcesDepartmentModel(Base, AuditMixin):
    
    __tablename__ = "department"
    __table_args__ = {"schema": "humanresources"}
    
    department_id = Column(Integer, primary_key= True, autoincrement= True)
    department_name = Column(String(100), nullable= False)
    group_name = Column(String(100), nullable= False)
    
    parent_department_id = Column(Integer, ForeignKey("humanresources.department.department_id"), nullable=True)
    manager_id = Column(Integer, ForeignKey("humanresources.employee.employee_id"), nullable=True)
    
    subdepartments = relationship(
        "HumanResourcesDepartmentModel", 
        backref="parent_department", # Creates the reverse path automatically
        remote_side=[department_id]  # Tells SQLAlchemy that the "remote" key is the ID itself
    )