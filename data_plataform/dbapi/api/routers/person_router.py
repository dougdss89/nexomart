from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession


# Dummy DB injection import, for reference of the Depends pattern only
async def get_db():
    yield None


from api.schemas.person.person_customer import Customer as PersonCustomer
from api.services.person.person_customer_service import PersonCustomerService

router = APIRouter(prefix="/customers", tags=["Customers"])


@router.get("/{id_customer}", response_model=PersonCustomer, status_code=status.HTTP_200_OK)
async def fetch_customer(id_customer: int, db: AsyncSession = Depends(get_db)):
    """
    100% Async.
    The database returns the BaseModel via Service.
    FastAPI serializes it automatically via 'response_model' thanks to Pydantic's 'from_attributes=True' Config!
    """
    try:
        customer_model = await PersonCustomerService.get_customer_profile(
            db=db, customer_id=id_customer
        )
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))

    if not customer_model:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Retail customer not listed in the systems.",
        )

    return customer_model


@router.post("/", response_model=PersonCustomer, status_code=status.HTTP_201_CREATED)
async def create_single_customer(payload: PersonCustomer, db: AsyncSession = Depends(get_db)):
    """
    Receives a REST request. Validates the typed Payload (via Pydantic DTO).
    Sends it as a model_dump() dictionary for the blind Service layer to persist.
    """
    validated_dict = payload.model_dump(exclude_unset=True)

    # Delegates business rule
    new_customer_created = await PersonCustomerService.register_new_customer(
        db=db, validated_payload=validated_dict
    )

    return new_customer_created


# --- SELLER ---
from api.schemas.sales.sales_seller import Seller as SellerSchema
from api.services.person.seller_service import SellerService


@router.get("/sellers/{seller_id}", response_model=SellerSchema)
async def get_seller(seller_id: int, db: AsyncSession = Depends(get_db)):
    """Retrieves the full profile of a seller"""
    service = SellerService(db)
    seller = await service.get_seller_profile(seller_id)
    if not seller:
        raise HTTPException(status_code=404, detail="Seller not found")
    return seller


@router.post("/sellers/bulk", status_code=status.HTTP_201_CREATED)
async def register_sellers_bulk(payload: list[SellerSchema], db: AsyncSession = Depends(get_db)):
    """Bulk ingestion of sellers (Partners and Stores)"""
    service = SellerService(db)
    await service.bulk_register_sellers(payload)
    return {"status": "success", "count": len(payload)}
