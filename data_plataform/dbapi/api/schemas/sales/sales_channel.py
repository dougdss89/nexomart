import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class ChannelType(str, Enum):
    WEB = "web"
    STORE = "store"
    APP = "app"
    MKT_PLACE = "marketplace"
    B2B = "b2b"


class SalesCurrency(str, Enum):
    BRL = "BRL"
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"


class SalesSettings(BaseModel):
    channel_type: ChannelType = Field(default=ChannelType.STORE)
    allows_return: bool = True
    requires_pos_auth: bool = False
    default_currency: SalesCurrency | None = SalesCurrency.BRL
    stock_source_priority: list[str] = Field(
        ...,
        min_length=1,
        description="Warehouse priority list (e.g.: ['warehouse_sp', 'store_01'])",
    )


class SalesChannelSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, use_enum_values=True)

    saleschannel_id: int = Field(gt=0)
    saleschannel_code: str = Field(max_length=20)
    saleschannel_name: str = Field(max_length=100)

    saleschannel_info: SalesSettings

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
