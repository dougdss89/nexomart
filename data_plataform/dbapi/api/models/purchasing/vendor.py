from sqlalchemy import Column, Integer, String, Index, text,  Numeric, Boolean, cast
from sqlalchemy.dialects.postgresql import JSONB
from api.models.base import Base, AuditMixin

class PurchaseVendorModel(Base, AuditMixin):
    
    __tablename__ = "vendor"
    
    vendor_id = Column(Integer, primary_key=True, autoincrement=True)
    business_entity_id = Column(Integer, nullable=False)
    account_number = Column(String(50), nullable=False)
    vendor_name = Column(String(100), nullable=False)
    
    credit_rating = Column(Numeric(19,4), nullable=False, default=0.00)
    
    purchasing_webservice_url = Column(String(200), nullable=True)
    is_active = Column(Boolean, nullable=False, default=True)

    vendor_details = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))

    effective_start_date = Column(DateTime, nullable=False)
    effective_end_date = Column(DateTime, nullable=True)
    is_current = Column(Boolean, nullable=False)
    
    __table_args__ = (
        Index('ix_vendor_details_lead_time', cast(vendor_details['average_lead_time_days'].astext, Integer)),
        Index('ix_vendor_certifications', cast(vendor_details['certifications'].astext)),
        Index('ix_vendor_primarycontact_name', cast(vendor_details['primary_contact']['name'].astext)),
        
        {"schema": "purchase"}
    )
    
