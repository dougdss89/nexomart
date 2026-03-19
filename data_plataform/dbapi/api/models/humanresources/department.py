from sqlalchemy import Column, Integer, String, ForeignKey
from api.models.base import AuditMixin, Base
from sqlalchemy.orm import relationship

class HumanResourcesDepartmentModel(Base, AuditMixin):
    
    __tablename__ = "department"
    __table_args__ = {"schema": "humanresources"}
    
    department_id = Column(Integer, primary_key= True, autoincrement= True)
    department_name = Column(String(100), nullable= False)
    group_name = Column(String(100), nullable= False)
    
    parent_department_id = Column(Integer, ForeignKey("human_resources.department.department_id"), nullable=True)
    manager_id = Column(Integer, nullable= True)
    
    subdepartments = relationship(
        "HumanResourcesDepartmentModel", 
        backref="parent_department", # Cria o caminho reverso automaticamente
        remote_side=[department_id]  # Diz ao SQLAlchemy que a chave "remota" é o próprio ID
    )