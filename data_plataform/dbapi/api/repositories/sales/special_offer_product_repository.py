from api.models.sales.specialofferproduct import SpecialOfferProductModel
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession


class SpecialOfferProductRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, sop: SpecialOfferProductModel) -> SpecialOfferProductModel:
        self.session.add(sop)
        return sop

    async def bulk_create(self, sop_list: list[dict]) -> None:
        if not sop_list:
            return
        stmt = insert(SpecialOfferProductModel).values(sop_list)
        await self.session.execute(stmt)

    async def get_products_by_offer(self, special_offer_id: int) -> list[SpecialOfferProductModel]:
        stmt = select(SpecialOfferProductModel).where(
            SpecialOfferProductModel.special_offer_id == special_offer_id
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def delete_mapping(self, special_offer_id: int, product_id: int) -> None:
        stmt = delete(SpecialOfferProductModel).where(
            SpecialOfferProductModel.special_offer_id == special_offer_id,
            SpecialOfferProductModel.product_id == product_id,
        )
        await self.session.execute(stmt)
