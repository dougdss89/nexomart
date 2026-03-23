import logging
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.purchasing.purchasing_vendor_repository import PurchasingVendorRepository

logger = logging.getLogger(__name__)

class PurchasingVendorService:
    """ Controle Analítico sobre Fornecedores de B2B """
    
    @staticmethod
    async def process_bulk_integration(db: AsyncSession, vendors_data: List[Dict[str, Any]]) -> bool:
        if not vendors_data:
            return False
            
        return await PurchasingVendorRepository.bulk_insert(db=db, vendors_data=vendors_data)
