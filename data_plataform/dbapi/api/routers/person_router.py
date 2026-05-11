from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Import Dummy de DB Injeção apenas para referência do Padrão Depends
async def get_db():
    yield None

from api.schemas.person.person_customer import Customer as PersonCustomer
from api.services.person.person_customer_service import PersonCustomerService

router = APIRouter(prefix="/customers", tags=["Customers"])

@router.get("/{id_customer}", response_model=PersonCustomer, status_code=status.HTTP_200_OK)
async def fetch_customer(id_customer: int, db: AsyncSession = Depends(get_db)):
    """
    100% Assíncrono.
    O banco devolve o BaseModel via Service.
    O FastApi via 'response_model' serializa automaticamente graças ao 'from_attributes=True' das Configs do Pydantic!
    """
    try:
        cliente_modelo = await PersonCustomerService.get_customer_profile(db=db, customer_id=id_customer)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    
    if not cliente_modelo:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cliente Varejista não listado nos sistemas.")
        
    return cliente_modelo


@router.post("/", response_model=PersonCustomer, status_code=status.HTTP_201_CREATED)
async def create_single_customer(payload: PersonCustomer, db: AsyncSession = Depends(get_db)):
    """
    Recebe requisição Rest. Valida Payload tipado (via DTO do Pydantic). 
    Envia como dicionário model_dump() para a camada cega do Service persistir.
    """
    dicionario_validado = payload.model_dump(exclude_unset=True)
    
    # Repassa regra de negócios
    novo_cliente_criado = await PersonCustomerService.register_new_customer(db=db, validated_payload=dicionario_validado)
    
    return novo_cliente_criado

# --- SELLER ---
from api.schemas.sales.sales_seller import Seller as SellerSchema
from api.services.person.seller_service import SellerService

@router.get("/sellers/{seller_id}", response_model=SellerSchema)
async def get_seller(seller_id: int, db: AsyncSession = Depends(get_db)):
    """ Recupera o perfil completo de um vendedor """
    service = SellerService(db)
    seller = await service.get_seller_profile(seller_id)
    if not seller:
        raise HTTPException(status_code=404, detail="Vendedor não encontrado")
    return seller

@router.post("/sellers/bulk", status_code=status.HTTP_201_CREATED)
async def register_sellers_bulk(payload: List[SellerSchema], db: AsyncSession = Depends(get_db)):
    """ Ingestão massiva de vendedores (Parceiros e Lojas) """
    service = SellerService(db)
    await service.bulk_register_sellers(payload)
    return {"status": "success", "count": len(payload)}
