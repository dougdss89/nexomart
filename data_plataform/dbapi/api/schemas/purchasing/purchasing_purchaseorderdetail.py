import uuid
from decimal import Decimal
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime

class PurchaseOrderDetailSchema(BaseModel):
    
    model_config = ConfigDict(from_attributes=True)
    
    # --- CHAVES ---
    purchaseorderdetail_id: str = Field(max_length=100)
    purchaseorderheader_id: str = Field(max_length=100, description="FK para PurchaseOrderHeader")
    product_id: int = Field(gt=0, description="FK para o Produto/Material")
    
    # --- DATAS ---
    due_date: datetime = Field(..., description="Data prometida para ESTE item")
    last_receipt_date: Optional[datetime] = Field(None, description="Data da última entrega real")
    
    # --- QUANTIDADES E VALORES ---
    order_qty: int = Field(gt=0, description="Quantidade pedida inicialmente")
    
    # Mantido com 4 casas decimais para consistência financeira com o Header
    unit_price: Decimal = Field(ge=0.0, decimal_places=4)
    
    # Recebimentos (Parciais ou Totais)
    received_qty: int = Field(default=0, ge=0, description="Quantidade que já chegou nas docas")
    rejected_qty: int = Field(default=0, ge=0, description="Quantidade devolvida pelo Controle de Qualidade")
    stocked_qty: int = Field(default=0, ge=0, description="Quantidade que foi para o estoque")
    
    # Flag facilitadora (Opcional na entrada, pois podemos calcular)
    is_fully_received: bool = Field(default=False)
    
    # --- AUDITORIA ---
    # Nota: No seu diagrama tinha 'modified_date' e 'updated_at'. 
    # Em Engenharia de Dados, usamos apenas 'updated_at' para evitar redundância.
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- VALIDAÇÕES DE LÓGICA DE ESTOQUE ---
    
    @model_validator(mode="after")
    def validate_receipt_logic(self):
        
        # Regra 1: Lógica Física (Não dá pra rejeitar mais do que chegou)
        if self.rejected_qty > self.received_qty:
            raise ValueError(f"A quantidade rejeitada ({self.rejected_qty}) não pode ser maior que a recebida ({self.received_qty}).")
            
        # Regra 2: Rastreabilidade Temporal
        if self.received_qty > 0 and self.last_receipt_date is None:
            raise ValueError("Se houve recebimento (received_qty > 0), a data da última entrega (last_receipt_date) deve ser informada obrigatoriamente.")

        # Regra 3: Consistência de Fechamento de Linha
        # A quantidade "aceita" (que vai para o estoque real) é o que chegou menos o que quebrou/falhou
        accepted_qty = self.received_qty - self.rejected_qty
        
        if self.is_fully_received and accepted_qty < self.order_qty:
            # Protege contra o fechamento prematuro de um pedido que ainda tem pendências
            raise ValueError(
                f"Item não pode ser marcado como 'is_fully_received=True'. "
                f"Quantidade aceita ({accepted_qty}) é menor que a pedida ({self.order_qty})."
            )

        # Regra 4: Consistência de Estoque
        if self.stocked_qty > self.received_qty:
            raise ValueError(f"A quantidade em estoque ({self.stocked_qty}) não pode ser maior que a recebida ({self.received_qty}).")
        
        if self.stocked_qty > self.order_qty:
            raise ValueError(f"A quantidade em estoque ({self.stocked_qty}) não pode ser maior que a pedida ({self.order_qty}).")

        # Regra 5: Consistência de Estoque
        if self.stocked_qty + self.rejected_qty > self.received_qty:
            raise ValueError(f"A quantidade em estoque ({self.stocked_qty}) + a quantidade rejeitada ({self.rejected_qty}) não pode ser maior que a recebida ({self.received_qty}).")
        
        if self.stocked_qty + self.rejected_qty > self.order_qty:
            raise ValueError(f"A quantidade em estoque ({self.stocked_qty}) + a quantidade rejeitada ({self.rejected_qty}) não pode ser maior que a pedida ({self.order_qty}).")

        return self