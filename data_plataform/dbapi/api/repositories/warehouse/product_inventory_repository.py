from api.models.warehouse.productinventory import WarehouseProductInventoryModel
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession


class ProductInventoryRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self, limit: int = 100) -> list[WarehouseProductInventoryModel]:
        query = select(WarehouseProductInventoryModel).limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, inventory_id: int) -> WarehouseProductInventoryModel | None:
        query = select(WarehouseProductInventoryModel).filter(
            WarehouseProductInventoryModel.inventory_id == inventory_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def get_by_product_and_location(
        self, product_id: int, location_id: int
    ) -> WarehouseProductInventoryModel | None:
        query = select(WarehouseProductInventoryModel).filter(
            WarehouseProductInventoryModel.product_id == product_id,
            WarehouseProductInventoryModel.warehouselocation_id == location_id,
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def bulk_upsert(self, data_list: list[dict]):
        """
        Insere novos registros de estoque em lote.
        """
        if not data_list:
            return

        stmt = insert(WarehouseProductInventoryModel).values(data_list)
        await self.session.execute(stmt)
        await self.session.flush()

    async def update_quantity(self, inventory_id: int, new_quantity: int):
        stmt = (
            update(WarehouseProductInventoryModel)
            .where(WarehouseProductInventoryModel.inventory_id == inventory_id)
            .values(quantity=new_quantity)
        )
        await self.session.execute(stmt)
