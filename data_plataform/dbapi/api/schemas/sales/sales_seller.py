import uuid
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime, date
from enum import Enum
from typing import Optional, List
from decimal import Decimal

class SellerType(str, Enum):
    
    BUSINESS = "business"
    INDIVIDUAL = "individual"
    
class AddressType(str, Enum):
    
    BILLING = "billing"
    SHIPPING = "shipping"
    OFFICE = "office"
    WAREHOUSE = "warehouse"
    
class SellerAccountType(str, Enum):
    
    CHECKING = "checking"
    SAVINGS = "savings"

class SellerBankAccount(BaseModel):
    
    bank_code: str = Field(max_length=10)
    agency: str = Field(max_length=10)
    account_number: str = Field(max_length=20)
    account_type: SellerAccountType
    transfer_key: Optional[str] = Field(None, max_length=100)
    
    
class SellerAddress(BaseModel):
    
    address_type: AddressType = Field(alias="address_type") # Maps 'type' from JSON to 'address_type' in Python if you want to avoid a conflict with a reserved word
    address_line: str = Field(max_length=150)
    postal_code: str = Field(max_length=20)
    street_number: str = Field(max_length=20)
    city: str = Field(max_length=150)
    state_name: str = Field(max_length=150)
    country: str = Field(max_length=150)
    
class SellerMetrics(BaseModel):
    
    average_rating: Decimal = Field(default=0.0, ge=0.0, decimal_places=1)
    total_gross_revenue: Decimal = Field(default=0.00, ge=0.0, decimal_places=2)
    total_items_sold: int = Field(ge=0)
    count_orders: int = Field(default=0, ge=0) # Name fixed to match the table
    chargeback_rate: Decimal = Field(default=0.00, ge=0.0, decimal_places=2)
    date_first_sale: date = Field(default_factory=date.today)
    date_last_sale: date = Field(default_factory=date.today)

class SellerDemographics(BaseModel):
    information: dict = Field(default_factory=dict, description="E.g.: {'sellertype': 'business'}")
    bank_account: SellerBankAccount
    address: List[SellerAddress] = Field(default_factory=list, description="List of addresses (Billing, Shipping, etc)")
    metrics: SellerMetrics
    
    
class Seller(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True, populate_by_name=True)
    
    seller_id: int = Field(gt=0)
    seller_name: str = Field(max_length=100)
    trading_name: str = Field(max_length=100)
    social_number: str = Field(max_length=20)
    opened_date: date = Field(default_factory=date.today)
    commission_rate: Decimal = Field(ge=0.0, decimal_places=4)
    country_id: int = Field(gt=0)
    demographics: SellerDemographics
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)