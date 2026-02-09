from datetime import date, datetime
from decimal import Decimal
from typing import Optional
import uuid
from pydantic import BaseModel, Field

class MarketingMetrics(BaseModel):
    # --- VIDEO (Funil de Retenção) ---
    video_views: int = Field(default=0, description="Visualizações totais (ex: 3s ou 2s)")
    video_views_25: int = Field(default=0)
    video_views_50: int = Field(default=0)
    video_views_75: int = Field(default=0) # O quartil que faltava
    video_views_100: int = Field(default=0)
    
    # --- ENGAJAMENTO SOCIAL ---
    likes: int = Field(default=0)
    comments: int = Field(default=0)
    shares: int = Field(default=0)
    saves: int = Field(default=0, description="Salvamentos (comum no Insta/TikTok)")
    clicks_all: int = Field(default=0, description="Cliques totais (inclui clique no perfil, não só link)")

    # --- FUNIL DE CONVERSÃO (Granularidade) ---
    adds_to_cart: int = Field(default=0)
    checkouts_initiated: int = Field(default=0)
    leads: int = Field(default=0)
    app_installs: int = Field(default=0)
    
    # --- COMPETITIVIDADE (Google Ads) ---
    # Usamos Float (0.0 a 1.0 ou percentual)
    search_impression_share: Optional[float] = Field(None)
    search_lost_is_budget: Optional[float] = Field(None)
    search_lost_is_rank: Optional[float] = Field(None)

    # --- METRICAS DE QUALIDADE ---
    quality_score: Optional[int] = Field(None, ge=1, le=10)
    frequency: float = Field(default=1.0)
    reach: int = Field(default=0, description="Alcance único diário")

    created_at: datetime = Field(default_factory=datetime.now)

class MarketingDailyPerformance(BaseModel):
    
    performance_id: int = Field(gt=0)
    campaign_id: int = Field(gt=0)
    ad_id: int = Field(gt=0)
    
    # Adicionei ad_group_id pois é o nível intermediário padrão (Hierarquia: Campanha > Grupo > Anúncio)
    ad_group_id: int = Field(gt=0) 
    
    # Use date do Python padrão para garantir serialização ISO (YYYY-MM-DD)
    date_ref: date = Field(..., description="Data de referência da performance") 
    
    # --- CORREÇÃO DE NOMES (Snake Case) ---
    # No banco de dados, colunas com letras maiúsculas (Cost) exigem aspas ("Cost").
    # O padrão em Data Engineering é sempre minúsculo (snake_case).
    
    cost: Decimal = Field(ge=0.0, decimal_places=4) # 4 casas decimais para CPMs baixos
    impressions: int = Field(ge=0)
    clicks: int = Field(ge=0)
    conversions: int = Field(ge=0) # 'Conv' é abreviado demais, prefira explicito
    conversion_value: Decimal = Field(ge=0.0, decimal_places=2)
    
    # Campo JSONB no Postgres
    metrics: MarketingMetrics = Field(default_factory=MarketingMetrics)
        
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    is_active: bool = True
    row_guid: uuid.UUID = Field(default_factory=uuid.uuid4)

    class Config:
        from_attributes = True