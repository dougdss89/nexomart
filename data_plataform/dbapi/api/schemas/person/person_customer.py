import uuid
from datetime import date, datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


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
    phone_number: str | None = None

class SocialProfile(BaseModel):
    gender: str | None = Field(None, max_length=20)
    marital_status: str | None = Field(None, max_length=10)
    interests: list[str] = Field(default_factory=list)
    occupation: str | None = Field(None, max_length=50)
    company_name: str | None = Field(None, max_length=100)
    tax_regime: TaxRegime | None = None
    buying_contact: BuyingContact | None = None  

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
    country_region: str | None = Field(None, max_length=150)
    province_name: str | None = Field(None, max_length=150)
    address_info: str | None = Field(None, max_length=150)

class CustomerMetrics(BaseModel):

    minimum_year_income: Decimal = Field(default=Decimal("0.00"), decimal_places=2)
    maximum_year_income: Decimal = Field(default=Decimal("0.00"), decimal_places=2)
    total_purchased: Decimal = Field(default=Decimal("0.00"), decimal_places=2)
    date_of_first_purchase: date | None = None
    date_last_purchase: date | None = None
    count_orders: int = Field(default=0)

class CustomerDemographics(BaseModel):

    social: SocialProfile | None = None
    marketing_consent: MarketingConsent | None = None
    address: Address | None = None
    metrics: CustomerMetrics | None = None

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
