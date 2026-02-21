import uuid
from decimal import Decimal
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, model_validator

class IoTSensorData(BaseModel):
    temp_at_scan: Optional[float] = Field(None, description="Temperatura no momento da bipagem")
    device_id: Optional[str] = Field(None, description="ID do scanner")

class TraceabilityInfo(BaseModel):
    reason: Optional[str] = Field(None, description="Motivo")
    performed_by_user: str = Field(description="Usuário responsável")
    source_warehouselocation_id: Optional[int] = None
    target_warehouselocation_id: Optional[int] = None
    quality_check_status: Optional[str] = Field(None)
    iot_sensor_data: Optional[IoTSensorData] = None

class TransactionHistorySchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    transaction_id: int = Field(gt=0)
    product_id: int = Field(gt=0)
    reference_order_id: Optional[int] = None
    reference_order_line_id: Optional[int] = None
    
    transaction_date: datetime = Field(default_factory=datetime.now)
    transactiontype_id: int = Field(gt=0)
    quantity: int = Field(..., description="Positivo para Entrada, Negativo para Saída")
    actual_cost: Decimal = Field(ge=0.0, decimal_places=4)
    
    traceability_info: TraceabilityInfo
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
    
    @model_validator(mode="after")
    def validate_transaction_logic(self):
        if self.quantity < 0:
            raise ValueError("A quantidade da transação não pode ser negativa.")
            
        if self.quantity == 0:
            raise ValueError("A quantidade da transação não pode ser zero.")
            
        info = self.traceability_info
        if info.source_warehouselocation_id and info.target_warehouselocation_id:
            if info.source_warehouselocation_id == info.target_warehouselocation_id:
                 raise ValueError("Origem e Destino não podem ser o mesmo local numa transferência.")
        return self