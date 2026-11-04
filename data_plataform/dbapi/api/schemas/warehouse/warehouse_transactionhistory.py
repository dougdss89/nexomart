import uuid
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class IoTSensorData(BaseModel):
    temp_at_scan: float | None = Field(None, description="Temperature at the moment of scanning")
    device_id: str | None = Field(None, description="Scanner ID")


class TraceabilityInfo(BaseModel):
    reason: str | None = Field(None, description="Reason")
    performed_by_user: str = Field(description="Responsible user")
    source_warehouselocation_id: int | None = None
    target_warehouselocation_id: int | None = None
    quality_check_status: str | None = Field(None)
    iot_sensor_data: IoTSensorData | None = None


class TransactionHistorySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str = Field(..., description="Pattern TR-XXXXXXXXX")
    product_id: int = Field(gt=0)
    reference_order_id: int | None = None
    reference_order_line_id: int | None = None

    transaction_date: datetime = Field(default_factory=datetime.now)
    transactiontype_id: int = Field(gt=0)
    quantity: int = Field(..., description="Positive for Inbound, Negative for Outbound")
    actual_cost: Decimal = Field(ge=0.0, decimal_places=4)

    traceability_info: TraceabilityInfo

    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    @model_validator(mode="after")
    def validate_transaction_logic(self):
        if self.quantity == 0:
            raise ValueError("The transaction quantity cannot be zero.")

        info = self.traceability_info
        if info.source_warehouselocation_id and info.target_warehouselocation_id:
            if info.source_warehouselocation_id == info.target_warehouselocation_id:
                raise ValueError(
                    "Source and Destination cannot be the same location in a transfer."
                )
        return self
