from api.models.sales.specialofferproduct import SpecialOfferProductModel
from api.repositories.sales.special_offer_product_repository import SpecialOfferProductRepository
from api.schemas.sales.sales_special_offer_product import (
    SpecialOfferProduct as SpecialOfferProductSchema,
)
from sqlalchemy.ext.asyncio import AsyncSession


class SpecialOfferProductService:
    def __init__(self, session: AsyncSession):
        self.repository = SpecialOfferProductRepository(session)

    async def list_products_by_offer(self, special_offer_id: int) -> list[SpecialOfferProductModel]:
        return await self.repository.get_products_by_offer(special_offer_id)

    async def bulk_map_products_to_offer(self, schemas: list[SpecialOfferProductSchema]) -> None:
        payloads = [schema.model_dump() for schema in schemas]
        await self.repository.bulk_create(payloads)

    async def unmap_product_from_offer(self, special_offer_id: int, product_id: int) -> None:
        await self.repository.delete_mapping(special_offer_id, product_id)
