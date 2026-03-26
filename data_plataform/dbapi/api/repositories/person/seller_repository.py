from typing import List, Optional
from sqlalchemy import select, insert
from sqlalchemy.ext.asyncio import AsyncSession
from api.models.person.seller import SellerModel

class SellerRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, seller: SellerModel) -> SellerModel:
        self.session.add(seller)
        return seller

    async def bulk_create(self, sellers: List[dict]) -> None:
        if not sellers:
            return
        stmt = insert(SellerModel).values(sellers)
        await self.session.execute(stmt)

    async def get_by_id(self, seller_id: int) -> Optional[SellerModel]:
        stmt = select(SellerModel).where(SellerModel.seller_id == seller_id)
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def list_all(self, limit: int = 100) -> List[SellerModel]:
        stmt = select(SellerModel).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())
