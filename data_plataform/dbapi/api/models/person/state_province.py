from api.config.base import AuditMixin, Base
from sqlalchemy import Column, ForeignKey, Integer, String


class StateProvinceModel(Base, AuditMixin):
    __tablename__ = "stateprovince"
    __table_args__ = {"schema": "person"}

    state_province_id = Column(Integer, primary_key=True, autoincrement=True)
    state_province_name = Column(String(100), nullable=False)
    state_province_code = Column(String(3), nullable=False)
    country_id = Column(Integer, ForeignKey("person.countryregion.country_id"), nullable=False)
