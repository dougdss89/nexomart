
from enum import Enum
from typing import Optional, List
import uuid
from pydantic import BaseModel, Field, AnyUrl, HttpUrl
from datetime import  datetime

class MarketingStatus(str, Enum):
    
    PLANNED = "planned"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    
class MarketingMediaType(str, Enum):
    
    VIDEO = "video"
    IMAGE = "image"
    CAROUSEL = "carousel"
    TEXT = "text"
    
class MediaAsset(BaseModel):
    # HttpUrl valida se é um link real (começa com http/https)
    # AnyUrl aceita s3://, gs://, etc.
    url: AnyUrl = Field(description="URL do S3 ou CDN") 
    
    # Opcionais, pois uma imagem não tem duração
    duration_seconds: Optional[int] = Field(None, ge=0, description="Duração em segundos (para vídeos)")
    file_size_mb: Optional[float] = Field(None, ge=0, description="Tamanho do arquivo em MB")
    width: Optional[int] = Field(None, description="Largura em pixels")
    height: Optional[int] = Field(None, description="Altura em pixels")
    
class MarketingCreativeData(BaseModel):
    
    headline: str = Field(max_length=150)
    description: str = Field(max_length=200)
    
    # Validação de URL real para o destino do anúncio
    final_url: HttpUrl 
    
    media_type: MarketingMediaType
    
    # AQUI ESTÁ A CORREÇÃO DE TIPAGEM:
    # Em vez de List[Dict], usamos List[MediaAsset]
    media_assets: List[MediaAsset] = Field(default_factory=list)
    

class MarketingAd(BaseModel):
    
    ad_id: int = Field(gt=0)
    ad_group_id: int = Field(gt=0)
    ad_name: str = Field(max_length=150)
    external_id: str = Field(max_length=20)
    
    utm_content: Optional[str] = Field(max_length=100)
    utm_medium:  Optional[str] = Field(max_length=100)
    
    ad_status: MarketingStatus
    
    creative_data: MarketingCreativeData
    
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)
        
    class Config:
        from_attributes = True
        use_enum_values = True