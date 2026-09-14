from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.sales.special_offer_repository import SpecialOfferRepository
from api.schemas.sales.sales_special_offer import SpecialOffer as SpecialOfferSchema
from api.models.sales.specialoffer import SpecialOfferModel

class SpecialOfferService:
    def __init__(self, session: AsyncSession):
        self.repository = SpecialOfferRepository(session)

    async def create_offer(self, schema: SpecialOfferSchema) -> SpecialOfferModel:
        offer = SpecialOfferModel(**schema.model_dump())
        return await self.repository.create(offer)

    async def bulk_create_offers(self, schemas: List[SpecialOfferSchema]) -> None:
        payloads = [schema.model_dump() for schema in schemas]
        await self.repository.bulk_create(payloads)

    async def get_offer(self, offer_id: int) -> Optional[SpecialOfferModel]:
        return await self.repository.get_by_id(offer_id)

    async def list_active(self) -> List[SpecialOfferModel]:
        return await self.repository.list_active_offers()
