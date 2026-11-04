import logging
from typing import Any

from api.repositories.warehouse.warehouse_location_repository import WarehouseLocationRepository
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class WarehouseLocationService:
    """Logical Orchestration of Locations and Inventory Points"""

    @staticmethod
    async def process_bulk_integration(
        db: AsyncSession, locations_data: list[dict[str, Any]]
    ) -> bool:
        if not locations_data:
            return False

        # Capacity validations or geographic business rules could go here
        return await WarehouseLocationRepository.bulk_insert(db=db, locations_data=locations_data)

    @staticmethod
    async def get_location(db: AsyncSession, warehouse_id: int):
        return await WarehouseLocationRepository.get_by_id(db=db, warehouse_id=warehouse_id)

    @staticmethod
    async def get_all_locations(db: AsyncSession, limit: int = 100):
        return await WarehouseLocationRepository.list_all(db=db, limit=limit)
