import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.warehouse.warehouse_location_repository import WarehouseLocationRepository

logger = logging.getLogger(__name__)

class WarehouseLocationService:
    """ Orquestração Lógica de Localizações e Pontos de Inventário """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, locations_data: List[Dict[str, Any]]) -> bool:
        if not locations_data:
            return False
        
        # Aqui poderiam entrar validações de capacidade ou regras de negócio geográficas
        return await WarehouseLocationRepository.bulk_insert(db=db, locations_data=locations_data)

    @staticmethod
    async def get_location(db: AsyncSession, warehouse_id: int):
        return await WarehouseLocationRepository.get_by_id(db=db, warehouse_id=warehouse_id)

    @staticmethod
    async def get_all_locations(db: AsyncSession, limit: int = 100):
        return await WarehouseLocationRepository.list_all(db=db, limit=limit)
