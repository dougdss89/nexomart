from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List

# Import Dummy de DB Injeção apenas para referência do Padrão Depends
async def get_db():
    yield None

from api.schemas.person.customer import PersonCustomer
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
