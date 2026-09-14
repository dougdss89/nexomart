from typing import List, Optional
from sqlalchemy import select, insert, update
from sqlalchemy.ext.asyncio import AsyncSession
from api.models.sales.specialoffer import SpecialOfferModel

class SpecialOfferRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, offer: SpecialOfferModel) -> SpecialOfferModel:
        self.session.add(offer)
        return offer

    async def bulk_create(self, offers: List[dict]) -> None:
        if not offers:
            return
        stmt = insert(SpecialOfferModel).values(offers)
        await self.session.execute(stmt)

    async def get_by_id(self, offer_id: int) -> Optional[SpecialOfferModel]:
        stmt = select(SpecialOfferModel).where(SpecialOfferModel.special_offer_id == offer_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def list_active_offers(self) -> List[SpecialOfferModel]:
        from datetime import datetime
        now = datetime.now()
        stmt = select(SpecialOfferModel).where(
            SpecialOfferModel.is_active == True,
            SpecialOfferModel.start_date <= now,
            (SpecialOfferModel.end_date == None) | (SpecialOfferModel.end_date >= now)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
