from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Import Dummy
async def get_db():
    yield None

from api.schemas.marketing.marketing_campaign import MarketingCampaign
from api.services.marketing.marketing_campaign_service import MarketingCampaignService

router = APIRouter(prefix="/marketing", tags=["Marketing"])

@router.post("/campaigns/onboarding", response_model=MarketingCampaign, status_code=status.HTTP_201_CREATED)
async def create_single_campaign(payload: MarketingCampaign, db: AsyncSession = Depends(get_db)):
    """
    100% Assíncrono. Recebe o Schema Pydantic estrito, valida a API HTTP 
    e envia o payload puro (Dict) para as lógicas de negócio abaixo (Services).
    """
    
    dict_payload = payload.model_dump(exclude_unset=True)
    
    # Aqui delegamos para o Repository (abstraído no Service) a execução.
    # Exemplo mockado de lógica interna:
    # return await MarketingCampaignService.create_single(db=db, campaign_data=dict_payload)
    
    return payload
