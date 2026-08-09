from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.warehouse.product_inventory_repository import ProductInventoryRepository
from api.schemas.warehouse.warehouse_productinventory import ProductInventorySchema
from api.models.warehouse.productinventory import WarehouseProductInventoryModel

class ProductInventoryService:
    def __init__(self, session: AsyncSession):
        self.repository = ProductInventoryRepository(session)

    async def list_inventory(self, limit: int = 100) -> List[WarehouseProductInventoryModel]:
        return await self.repository.get_all(limit=limit)

    async def restock_inventory(self, items: List[ProductInventorySchema]):
        """
        Performs the initial load or bulk restocking of inventory.
        """
        # Converts schemas into plain dictionaries. The `attributes` field (JSONB) needs
        # to be serialized separately in "json" mode — a plain model_dump() keeps native
        # `date` values inside it, which asyncpg's default JSON encoder does not accept.
        data_list = []
        for item in items:
            data = item.model_dump()
            if item.attributes is not None:
                data["attributes"] = item.attributes.model_dump(mode="json")
            if data.get("inventory_id") is None:
                data.pop("inventory_id", None)  # let the database autoincrement take over
            data_list.append(data)
        await self.repository.bulk_upsert(data_list)

    async def update_stock(self, product_id: int, location_id: int, quantity_change: int):
        """
        Updates the stock balance for a product at a specific location.
        """
        inventory = await self.repository.get_by_product_and_location(product_id, location_id)
        if not inventory:
            raise ValueError(f"Inventory not found for product {product_id} at location {location_id}")

        new_qty = inventory.quantity + quantity_change
        if new_qty < 0:
            raise ValueError("Stock balance cannot be negative after the operation.")
            
        await self.repository.update_quantity(inventory.inventory_id, new_qty)
