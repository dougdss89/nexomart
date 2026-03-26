from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from api.config.base import Base, AuditMixin


class StateProvinceModel(Base, AuditMixin):
    
    __tablename__ = "stateprovince"
    __table_args__ = ({"schema": "person"})
    
    state_province_id = Column(Integer, primary_key=True, autoincrement=True)
    state_province_name = Column(String(100), nullable=False)
    state_province_code = Column(String(3), nullable=False)
    country_id = Column(Integer, ForeignKey("person.countryregion.country_id"), nullable=False)
    
    
 