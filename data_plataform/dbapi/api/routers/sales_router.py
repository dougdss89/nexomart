from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.schemas.sales.sales_channel import SalesChannelSchema
from api.services.sales.sales_channel_service import SalesChannelService

from api.schemas.sales.sales_eventlog import SalesOrderEventLogSchema
from api.services.sales.sales_event_log_service import SalesEventLogService

from api.schemas.sales.sales_order import SalesOrderSchema
from api.services.sales.sales_order_service import SalesOrderService

router = APIRouter(prefix="/sales", tags=["Sales"])

@router.get("/channels", response_model=List[SalesChannelSchema])
async def list_sales_channels(db: AsyncSession = Depends(get_db)):
    """ Lists all available sales channels """
    service = SalesChannelService(db)
    return await service.list_channels()

@router.post("/channels/bulk", status_code=status.HTTP_201_CREATED)
async def create_sales_channels_bulk(payload: List[SalesChannelSchema], db: AsyncSession = Depends(get_db)):
    """ Bulk load of sales channels (Master Data) """
    service = SalesChannelService(db)
    try:
        await service.seed_initial_channels(payload)
        return {"status": "success", "count": len(payload)}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# --- EVENT LOG ---
@router.get("/events/{order_id}", response_model=List[SalesOrderEventLogSchema])
async def get_order_events(order_id: int, db: AsyncSession = Depends(get_db)):
    """ Retrieves the audit trail (events) of a specific order """
    service = SalesEventLogService(db)
    return await service.get_order_history(order_id)

@router.post("/events/bulk", status_code=status.HTTP_201_CREATED)
async def create_events_bulk(payload: List[SalesOrderEventLogSchema], db: AsyncSession = Depends(get_db)):
    """ Bulk ingestion of Event Logs (Order Trail) """
    service = SalesEventLogService(db)
    await service.process_bulk_events(payload)
    return {"status": "success", "count": len(payload)}

# --- SALES ORDER ---
@router.get("/orders/{order_id}", response_model=SalesOrderSchema)
async def get_sales_order(order_id: int, db: AsyncSession = Depends(get_db)):
    """ Retrieves a complete sales order """
    service = SalesOrderService(db)
    order = await service.get_order(order_id)
    if not order:
        raise HTTPException(status_code=404, detail="Sales report not found")
    return order

@router.post("/orders/bulk", status_code=status.HTTP_201_CREATED)
async def ingest_sales_orders_bulk(payload: List[SalesOrderSchema], db: AsyncSession = Depends(get_db)):
    """ Ingestion of sales orders (Bulk Fact Table) """
    service = SalesOrderService(db)
    # Convert Pydantic to dict for the Service
    data = [item.model_dump() for item in payload]
    await service.bulk_ingest_orders(data)
    return {"status": "success", "count": len(payload)}

# --- RETURN ORDER ---
from api.schemas.sales.sales_return_order import SalesReturnOrderSchema
from api.services.sales.return_order_service import ReturnOrderService

@router.get("/returns/{return_id}", response_model=SalesReturnOrderSchema)
async def get_return_order(return_id: str, db: AsyncSession = Depends(get_db)):
    """ Retrieves the details of a specific return """
    service = ReturnOrderService(db)
    ret = await service.get_return(return_id)
    if not ret:
        raise HTTPException(status_code=404, detail="Return order not found")
    return ret

@router.post("/returns/bulk", status_code=status.HTTP_201_CREATED)
async def ingest_return_orders_bulk(payload: List[SalesReturnOrderSchema], db: AsyncSession = Depends(get_db)):
    """ Bulk ingestion of return orders (Fact Table) """
    service = ReturnOrderService(db)
    await service.bulk_create_returns(payload)
    return {"status": "success", "count": len(payload)}

# --- SPECIAL OFFER PRODUCT ---
from api.schemas.sales.sales_special_offer_product import SpecialOfferProduct as SpecialOfferProductSchema
from api.services.sales.special_offer_product_service import SpecialOfferProductService

@router.get("/offers/{offer_id}/products", response_model=List[SpecialOfferProductSchema])
async def list_offer_products(offer_id: int, db: AsyncSession = Depends(get_db)):
    """ Lists all products linked to a specific offer """
    service = SpecialOfferProductService(db)
    return await service.list_products_by_offer(offer_id)

@router.post("/offers/products/bulk", status_code=status.HTTP_201_CREATED)
async def map_offer_products_bulk(payload: List[SpecialOfferProductSchema], db: AsyncSession = Depends(get_db)):
    """ Bulk linking of products to special offers """
    service = SpecialOfferProductService(db)
    await service.bulk_map_products_to_offer(payload)
    return {"status": "success", "count": len(payload)}

# --- SPECIAL OFFER ---
from api.schemas.sales.sales_special_offer import SpecialOffer as SpecialOfferSchema
from api.services.sales.special_offer_service import SpecialOfferService

@router.get("/offers/{offer_id}", response_model=SpecialOfferSchema)
async def get_special_offer(offer_id: int, db: AsyncSession = Depends(get_db)):
    """ Retrieves the details of a specific special offer """
    service = SpecialOfferService(db)
    offer = await service.get_offer(offer_id)
    if not offer:
        raise HTTPException(status_code=404, detail="Special offer not found")
    return offer

@router.post("/offers/bulk", status_code=status.HTTP_201_CREATED)
async def create_special_offers_bulk(payload: List[SpecialOfferSchema], db: AsyncSession = Depends(get_db)):
    """ Bulk ingestion of special offers (Master Data) """
    service = SpecialOfferService(db)
    await service.bulk_create_offers(payload)
    return {"status": "success", "count": len(payload)}
