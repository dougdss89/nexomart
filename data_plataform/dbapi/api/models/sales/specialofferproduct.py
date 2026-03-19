from sqlalchemy import Column, Integer, ForeignKey
from api.models.base import AuditMixin, Base

class SpecialOfferProductModel(Base, AuditMixin):
    
    __tablename__ = "specialofferproduct"
    __table_args__ = {"schema": "sales"}
    
    specialoffer_product_id = Column(Integer, primary_key=True, autoincrement=True)
    special_offer_id = Column(Integer,  ForeignKey("sales.specialoffer.special_offer_id"), nullable= False)
    product_id = Column(Integer,        ForeignKey("product.product.product_id"), nullable=False)
    