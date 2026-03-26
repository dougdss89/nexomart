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
        Realiza a carga inicial ou reposição de estoque em lote.
        """
        # Converte schemas em dicionários puros
        data_list = [item.model_dump() for item in items]
        await self.repository.bulk_upsert(data_list)

    async def update_stock(self, product_id: int, location_id: int, quantity_change: int):
        """
        Atualiza o saldo de estoque para um produto em um local específico.
        """
        inventory = await self.repository.get_by_product_and_location(product_id, location_id)
        if not inventory:
            raise ValueError(f"Estoque não encontrado para produto {product_id} no local {location_id}")
        
        new_qty = inventory.quantity + quantity_change
        if new_qty < 0:
            raise ValueError("Saldo de estoque não pode ser negativo após a operação.")
            
        await self.repository.update_quantity(inventory.inventory_id, new_qty)
