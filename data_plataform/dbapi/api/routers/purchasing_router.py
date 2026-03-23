from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Import Dummy Setup
async def get_db():
    yield None

from api.schemas.purchasing.vendor import Vendor
from api.services.purchasing.purchasing_vendor_service import PurchasingVendorService

router = APIRouter(prefix="/purchasing", tags=["Purchasing"])

@router.post("/vendors/register", response_model=Vendor, status_code=status.HTTP_201_CREATED)
async def register_vendor(payload: Vendor, db: AsyncSession = Depends(get_db)):
    """
    Controlador Assíncrono de Ingestão de Fornecedores.
    Abstraído do banco de dados, confia cegamente que o Schema Payload
    traduz exatamente as regras dimensionais de B2B aprovadas.
    """
    
    dict_payload = payload.model_dump(exclude_unset=True)
    
    # Ordem simulada abstraindo o banco nativo
    # return await PurchasingVendorService.create_single(db=db, vendor_data=dict_payload)
    
    return payload
