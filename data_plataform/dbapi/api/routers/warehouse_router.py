# Import Database dependency
from api.database import get_db
from api.schemas.warehouse.warehouse_productinventory import ProductInventorySchema
from api.schemas.warehouse.warehouse_transaction_type import TransactionTypeSchema
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema
from api.schemas.warehouse.warehouse_wrhlocation import WarehouseLocationSchema
from api.services.warehouse.product_inventory_service import ProductInventoryService
from api.services.warehouse.transaction_history_service import TransactionHistoryService
from api.services.warehouse.transaction_type_service import TransactionTypeService
from api.services.warehouse.warehouse_location_service import WarehouseLocationService
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/warehouse", tags=["Warehouse"])


# --- WAREHOUSE LOCATIONS ---
@router.post("/locations/bulk", status_code=status.HTTP_201_CREATED)
async def create_locations_bulk(
    payload: list[WarehouseLocationSchema], db: AsyncSession = Depends(get_db)
):
    """Bulk ingestion of Warehouse Locations"""
    dict_payload = [item.model_dump(exclude_unset=True) for item in payload]
    success = await WarehouseLocationService.process_bulk_integration(
        db=db, locations_data=dict_payload
    )
    if not success:
        raise HTTPException(status_code=400, detail="Error processing integration")
    return {"status": "success", "count": len(dict_payload)}


@router.get("/locations", response_model=list[WarehouseLocationSchema])
async def list_warehouse_locations(limit: int = 100, db: AsyncSession = Depends(get_db)):
    return await WarehouseLocationService.get_all_locations(db=db, limit=limit)


# --- TRANSACTION TYPES ---
@router.get("/transaction-types", response_model=list[TransactionTypeSchema])
async def list_transaction_types(db: AsyncSession = Depends(get_db)):
    service = TransactionTypeService(db)
    return await service.list_transaction_types()


@router.post("/transaction-types/bulk", status_code=status.HTTP_201_CREATED)
async def create_transaction_types_bulk(
    payload: list[TransactionTypeSchema], db: AsyncSession = Depends(get_db)
):
    service = TransactionTypeService(db)
    await service.seed_initial_data(payload)
    return {"status": "success", "count": len(payload)}


# --- TRANSACTION HISTORY ---
@router.get("/transactions", response_model=list[TransactionHistorySchema])
async def list_transactions(limit: int = 100, db: AsyncSession = Depends(get_db)):
    service = TransactionHistoryService(db)
    return await service.list_transactions(limit=limit)


@router.post("/transactions/bulk", status_code=status.HTTP_201_CREATED)
async def create_transactions_bulk(
    payload: list[TransactionHistorySchema], db: AsyncSession = Depends(get_db)
):
    service = TransactionHistoryService(db)
    await service.process_bulk_transactions(payload)
    return {"status": "success", "count": len(payload)}


# --- PRODUCT INVENTORY ---
@router.get("/inventory", response_model=list[ProductInventorySchema])
async def list_inventory(limit: int = 100, db: AsyncSession = Depends(get_db)):
    """Queries the current inventory balance"""
    service = ProductInventoryService(db)
    return await service.list_inventory(limit=limit)


@router.post("/inventory/restock", status_code=status.HTTP_201_CREATED)
async def restock_inventory(
    payload: list[ProductInventorySchema], db: AsyncSession = Depends(get_db)
):
    """Initial load or restock of inventory balance"""
    service = ProductInventoryService(db)
    await service.restock_inventory(payload)
    return {"status": "success", "count": len(payload)}
