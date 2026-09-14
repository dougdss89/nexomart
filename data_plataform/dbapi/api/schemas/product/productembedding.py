import uuid
from typing import List, Optional
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime

# --- SUB-MODELO: Metadados de IA (Rastreabilidade) ---
class AIModelMetadata(BaseModel):
    text_model: Optional[str] = Field(None, example="openai-text-embedding-3-small")
    image_model: Optional[str] = Field(None, example="openai-clip-vit-base-patch32")
    
    # Optional: Useful to know whether the vector was generated in batch (ETL) or real-time (API)
    generation_method: str = Field(default="batch_pipeline")

# --- MODELO PRINCIPAL ---
class ProductEmbeddingSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    
    embedding_id: int = Field(gt=0)
    product_id: int = Field(gt=0, description="FK to product.product")

    # --- VECTORS (Lists of Floats) ---
    # We ensure the API didn't send a truncated vector or one from the wrong model
    description_vector: Optional[List[float]] = Field(
        None,
        min_length=1536,
        max_length=1536,
        description="Semantic Text Vector"
    )

    image_vector: Optional[List[float]] = Field(
        None,
        min_length=512,
        max_length=512,
        description="Visual Search Vector"
    )

    # --- CACHE / ETL CONTROL ---
    # SHA256 hash of (Name + Description) used to generate the vector.
    # Vital for the pipeline to know when to recalculate!
    source_data_hash: str = Field(
        max_length=64,
        description="Hash of the original data for update control"
    )

    # JSONB with AI traceability
    ai_metadata: AIModelMetadata = Field(default_factory=AIModelMetadata)
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    # --- GLOBAL VALIDATIONS ---
    @model_validator(mode="after")
    def validate_embedding_presence(self):
        # It doesn't make sense to have a row in this table without at least one vector
        if not self.description_vector and not self.image_vector:
            raise ValueError("The record must contain at least one vector (text or image).")
        return self