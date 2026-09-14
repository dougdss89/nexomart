from sqlalchemy import Column, Integer, String
from api.config.base import AuditMixin, Base


class CountryRegionModel(Base, AuditMixin):
    
    __tablename__ = "countryregion"
    __table_args__ = ({"schema": "person"})
    
    country_id = Column(Integer, primary_key=True, autoincrement=True)
    country_name = Column(String(100), nullable=False)
    country_code = Column(String(3), nullable=False)
    countryregion = Column(String(100), nullable=False)
    

    