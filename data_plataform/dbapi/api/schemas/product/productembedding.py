import uuid
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime

# --- SUB-MODELO: Metadados de IA (Rastreabilidade) ---
class AIModelMetadata(BaseModel):
    text_model: Optional[str] = Field(None, example="openai-text-embedding-3-small")
    image_model: Optional[str] = Field(None, example="openai-clip-vit-base-patch32")
    
    # Opcional: Útil para saber se o vetor foi gerado em batch (ETL) ou real-time (API)
    generation_method: str = Field(default="batch_pipeline")

# --- MODELO PRINCIPAL ---
class ProductEmbeddingSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    embedding_id: int = Field(gt=0)
    product_id: int = Field(gt=0, description="FK para product.product")
    
    # --- VETORES (Listas de Floats) ---
    # Garantimos que a API não mandou um vetor cortado ou de modelo errado
    description_vector: Optional[List[float]] = Field(
        None, 
        min_length=1536, 
        max_length=1536, 
        description="Vetor Semântico de Texto"
    )
    
    image_vector: Optional[List[float]] = Field(
        None, 
        min_length=512, 
        max_length=512, 
        description="Vetor de Busca Visual"
    )
    
    # --- CONTROLE DE CACHE / ETL ---
    # Hash SHA256 do (Nome + Descrição) usado para gerar o vetor. 
    # Vital para o pipeline saber quando recalcular!
    source_data_hash: str = Field(
        max_length=64, 
        description="Hash do dado original para controle de atualização"
    )
    
    # JSONB com rastreabilidade da IA
    ai_metadata: AIModelMetadata = Field(default_factory=AIModelMetadata)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- VALIDAÇÕES GLOBAIS ---
    @model_validator(mode="after")
    def validate_embedding_presence(self):
        # Não faz sentido ter uma linha nessa tabela se não houver pelo menos um vetor
        if not self.description_vector and not self.image_vector:
            raise ValueError("O registro deve conter pelo menos um vetor (texto ou imagem).")
        return self