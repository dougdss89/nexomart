import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.humanresources.humanresources_job_role_repository import HumanResourcesJobRoleRepository

logger = logging.getLogger(__name__)

class HumanResourcesJobRoleService:
    """ Orquestração dos Cargos (Histórico Salarial e Títulos) """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, roles_data: List[Dict[str, Any]]) -> bool:
        if not roles_data:
            return False
            
        return await HumanResourcesJobRoleRepository.bulk_insert(db=db, roles_data=roles_data)
