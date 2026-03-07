from sqlalchemy import Column, Integer, Foreign_key
from api.models.base import AuditMixin, Base

class SpecialOfferProductModel(Base, AuditMixin):
    
    __tablename__ = "specialofferproduct"
    __table_args__ = {"schema": "sales"}
    
    specialoffer_product_id = Column(Integer, primary_key=True, autoincrement=True)
    special_offer_id = Column(Integer,  Foreign_key("sales.specialoffer.special_offer_id"), nullable= False)
    product_id = Column(Integer,        Foreign_key("product.product.product_id"), nullable=False)
    