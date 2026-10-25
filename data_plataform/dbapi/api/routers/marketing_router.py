from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession


# Import Dummy
async def get_db():
    yield None


from api.schemas.marketing.marketing_campaign import MarketingCampaign

router = APIRouter(prefix="/marketing", tags=["Marketing"])


@router.post(
    "/campaigns/onboarding", response_model=MarketingCampaign, status_code=status.HTTP_201_CREATED
)
async def create_single_campaign(payload: MarketingCampaign, db: AsyncSession = Depends(get_db)):
    """
    100% Async. Receives the strict Pydantic Schema, validates the HTTP API
    and sends the plain payload (Dict) to the business logic below (Services).
    """

    dict_payload = payload.model_dump(exclude_unset=True)

    # Here we delegate execution to the Repository (abstracted in the Service).
    # Mocked example of internal logic:
    # return await MarketingCampaignService.create_single(db=db, campaign_data=dict_payload)

    return payload
