from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Import Dummy Setup
async def get_db():
    yield None

from api.schemas.purchasing.purchasing_vendor import Vendor
from api.services.purchasing.purchasing_vendor_service import PurchasingVendorService

router = APIRouter(prefix="/purchasing", tags=["Purchasing"])

@router.post("/vendors/register", response_model=Vendor, status_code=status.HTTP_201_CREATED)
async def register_vendor(payload: Vendor, db: AsyncSession = Depends(get_db)):
    """
    Async controller for Vendor ingestion.
    Abstracted from the database, blindly trusts that the Payload Schema
    exactly translates the approved B2B dimensional rules.
    """

    dict_payload = payload.model_dump(exclude_unset=True)

    # Simulated order abstracting the native database
    # return await PurchasingVendorService.create_single(db=db, vendor_data=dict_payload)
    
    return payload
