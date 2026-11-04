from api.models.person.seller import SellerModel
from api.repositories.person.seller_repository import SellerRepository
from api.schemas.sales.sales_seller import Seller as SellerSchema
from sqlalchemy.ext.asyncio import AsyncSession


class SellerService:
    def __init__(self, session: AsyncSession):
        self.repository = SellerRepository(session)

    async def register_seller(self, schema: SellerSchema) -> SellerModel:
        seller_data = schema.model_dump(exclude={"demographics"})
        seller_data["demographics"] = schema.demographics.model_dump()
        seller = SellerModel(**seller_data)
        return await self.repository.create(seller)

    async def bulk_register_sellers(self, schemas: list[SellerSchema]) -> None:
        payloads = []
        for schema in schemas:
            data = schema.model_dump(exclude={"demographics"})
            data["demographics"] = schema.demographics.model_dump()
            payloads.append(data)
        await self.repository.bulk_create(payloads)

    async def get_seller_profile(self, seller_id: int) -> SellerModel | None:
        return await self.repository.get_by_id(seller_id)

    async def list_sellers(self, limit: int = 100) -> list[SellerModel]:
        return await self.repository.list_all(limit)
