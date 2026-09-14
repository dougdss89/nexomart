from sqlalchemy import Column, Integer, String, DateTime, text, Index, Numeric, cast, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from api.config.base import AuditMixin, Base

class SellerModel(Base, AuditMixin):
    
    __tablename__ = "seller"
    
    seller_id = Column(Integer, primary_key=True, autoincrement=True)
    seller_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    social_number = Column(String(15), nullable=False)
    
    opened_date = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    commission_rate = Column(Numeric(19, 4), nullable=False, default=0.0)
        
    country_id = Column(Integer, ForeignKey("person.countryregion.country_id"), nullable=False )
    
    demographics = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    
    __table_args__ = (
        Index('ix_seller_demographics_information_sellertype', cast(demographics['information']['sellertype'].astext, String)),
        Index('ix_seller_demographics_bank_account_bank_code', cast(demographics['bank_account']['bank_code'].astext, String)),

        Index('ix_seller_demographics_address_country', cast(demographics['address'][6]['country'].astext, String)),
        Index('ix_seller_demographics_address_address_type', cast(demographics['address'][0]['address_type'].astext, String)),
        Index('ix_seller_demographics_address_city', cast(demographics['address'][4]['city'].astext, String)),
        Index('ix_seller_demographics_address_state_name', cast(demographics['address'][5]['state_name'].astext, String)),

        Index('ix_seller_demographics_metrics_total_sold', cast(demographics['metrics']['total_items_sold'].astext, Integer)),
        Index('ix_seller_demographics_metrics_count_orders', cast(demographics['metrics']['count_orders'].astext, Integer)),
        Index('ix_seller_demographics_metrics_date_first_sale', cast(demographics['metrics']['date_first_sale'].astext, String)),
        Index('ix_seller_demographics_metrics_date_last_sale',  cast(demographics['metrics']['date_last_sale'].astext, String)),

        {"schema": "person"})