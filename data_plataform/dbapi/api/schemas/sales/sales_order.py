import uuid
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import Optional, List

class DeliveryStatus(str, Enum):
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    DELIVERED = "delivered"
    RETURNED = "returned"
    PENDING = "pending"
    CANCELED = "canceled"
    
class TaxName(str, Enum):
    ICMS = "ICMS"
    IPI = "IPI"
    PIS = "PIS"
    COFINS = "COFINS"   
    
class ShippingMethod(str, Enum):
    GROUND = "ground"
    AIR = "air"
    SEA = "sea"
    
class ShippingTier(str, Enum):
    NORMAL = "normal"
    EXPRESS = "express"
    PRIORITY = "priority"
    SCHEDULED = "scheduled"
    
class SalesOrderStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    CANCELED = "canceled"
    PAYMENT_PENDING = "payment_pending"
    PARTIAL_PAYMENT = "partial_payment"
    PARTIAL_SHIPPED = "partial_shipped"
    PARTIAL_DELIVERED = "partial_delivered"
    PARTIAL_RETURNED = "partial_returned"
    FINALIZED = "finalized"
    
class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    BANK_TRANSFER = "bank_transfer"
    CASH = "cash" 
    PIX = "pix"
    DIGITAL_WALLET = "digital_wallet"
    
class PaymentStatus(str, Enum):
    PAID = "paid"
    PARTIALLY_PAID = "partially_paid"
    UNPAID = "unpaid"
    REFUNDED = "refunded"
    
class TaxItem(BaseModel):
    name: TaxName
    rate: Decimal = Field(ge=0.0, decimal_places=4, description="E.g.: 0.18 for 18%")
    amount: Decimal = Field(ge=0.0, decimal_places=4)

class DeliveryInfo(BaseModel):
    status: DeliveryStatus = Field(default=DeliveryStatus.PENDING)
    shipdate: Optional[date] = None
    estimated_date: Optional[date] = None
    shipping_provider: Optional[str] = Field(None, max_length=50)
    tracking_code: Optional[str] = Field(None, max_length=100)
    shipping_method: Optional[ShippingMethod] = None
    shipping_tier: Optional[ShippingTier] = None

class OrderItem(BaseModel):
    line_number: str = Field(..., description="Pattern SO-XXXXXXXXX-XX")
    line_id: int = Field(gt=0, description="Logical suffix of the item, e.g.: 1, 2, 3")
    sku: str = Field(max_length=50)
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0)
    unit_price: Decimal = Field(ge=0.0, decimal_places=4)
    seller_id: int = Field(gt=0, description="Who delivers (Marketplace Partner or Own Store)")
    
    # Sub-structs
    delivery_info: DeliveryInfo = Field(default_factory=DeliveryInfo)
    taxes: List[TaxItem] = Field(default_factory=list)

class PaymentTransaction(BaseModel):
    method: PaymentMethod
    amount: Decimal = Field(gt=0.0, decimal_places=4)
    gateway_id: Optional[str] = Field(None, max_length=100)
    status: PaymentStatus

class PaymentInfo(BaseModel):
    is_split: bool = False
    transactions: List[PaymentTransaction] = Field(default_factory=list)
    
class SalesOrderSchema(BaseModel):
    model_config = ConfigDict(from_attributes = True,
                               use_enum_values = True)
    
    salesorder_id: int = Field(gt=0)
    salesordernumber: str = Field(max_length=20)
    customer_id: int = Field(gt=0)
    saleschannel_id: int = Field(gt=0)
    specialoffer_id: int = Field(gt=0)
    
    store_id: int = Field(gt=0)
    order_date: datetime = Field(default_factory=datetime.now)

    order_status: SalesOrderStatus
    total_amount: Decimal = Field(ge=0.0, decimal_places=2)
    currency_code: str = Field(default="BRL", max_length=3)
    
    order_items: List[OrderItem] = Field(..., min_length = 1)
    payment_info: PaymentInfo = Field(default_factory = PaymentInfo)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    
# --- GLOBAL ENGINEERING VALIDATIONS ---
    @model_validator(mode="after")
    def validate_order_consistency(self):

        # 1. Financial Validation: Does the sum of the items match the order total?
        # (We sum unit_price * quantity for each item)
        items_total = sum(item.unit_price * item.quantity for item in self.order_items)
        if abs(self.total_amount - items_total) > Decimal("0.01"):
            pass
            # Tip: In prod, only enable this if you're sure how header discounts work.
            # raise ValueError(f"Sum of items ({items_total}) differs from total_amount ({self.total_amount})")

        # 2. Split Payment Logic Validation
        # If is_split is False, there shouldn't be more than 1 transaction
        if not self.payment_info.is_split and len(self.payment_info.transactions) > 1:
            raise ValueError("Payment contains multiple transactions but 'is_split' is false.")
            
        return self