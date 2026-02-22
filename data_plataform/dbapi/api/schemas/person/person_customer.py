import uuid
from pydantic import BaseModel, Field
from datetime import datetime, date
from enum import Enum
from typing import Optional, List
from decimal import Decimal


class CustomerType(str, Enum):
    STORE_CONTACT = "SC"
    VENDOR_CONTACT = "VC"
    
class TaxRegime(str, Enum):
    SIMPLE = "simple"
    PROFIT_REAL = "real_profit"
    PRESUMED = "presumed_profit"
    
class BuyingContact(BaseModel):
    name: str = Field(..., max_length=100)
    role: str = Field(..., max_length=100)
    phone_number: Optional[str] = None
    
class SocialProfile(BaseModel):
    gender: Optional[str] = Field(None, max_length=20)
    marital_status: Optional[str] = Field(None, max_length=10)
    interests: List[str] = Field(default_factory=list) # Lista de strings
    occupation: Optional[str] = Field(None, max_length=50)
    company_name: Optional[str] = Field(None, max_length=100)
    tax_regime: Optional[TaxRegime] = None
    buying_contact: Optional[BuyingContact] = None  
    
class MarketingConsent(BaseModel):
    email: bool = False
    sms: bool = False

class Address(BaseModel):
    address_line: str = Field(..., max_length=150)
    postal_code: str = Field(..., max_length=20)
    street_number: str = Field(..., max_length=20)
    city: str = Field(..., max_length=150)
    state_name: str = Field(..., max_length=150)
    country: str = Field(..., max_length=150)
    country_region: Optional[str] = Field(None, max_length=150)
    province_name: Optional[str] = Field(None, max_length=150)
    address_info: Optional[str] = Field(None, max_length=150)

class CustomerMetrics(BaseModel):

    minimum_year_income: Decimal = Field(default=0.00, decimal_places=2)
    maximum_year_income: Decimal = Field(default=0.00, decimal_places=2)
    total_purchased: Decimal = Field(default=0.00, decimal_places=2)
    date_of_first_purchase: Optional[date] = None
    date_last_purchase: Optional[date] = None
    count_orders: int = Field(default=0)
   
    
class CustomerDemographics(BaseModel):
    
    social: Optional[SocialProfile] = None
    marketing_consent: Optional[MarketingConsent] = None
    address: Optional[Address] = None
    metrics: Optional[CustomerMetrics] = None
    
    
class Customer(BaseModel):
    
    customer_id: int = Field(gt=0)
    customer_name: str = Field(max_length = 100)
    last_name: str = Field(max_length = 100)
    social_number: str = Field(max_length = 20)
    birthdate: date
    customer_type: CustomerType
    
    credit_limit: float = Field(ge=0)
    
    demographics: CustomerDemographics
    country_id: int = Field(gt=0)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active:bool = True
    row_guid:uuid.UUID = Field(default_factory=uuid.uuid4)
    
class Config:
    use_enum_values = True