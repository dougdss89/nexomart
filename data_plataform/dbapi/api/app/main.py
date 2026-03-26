from fastapi import FastAPI
from api.routers import (
    warehouse_router, 
    marketing_router, 
    person_router, 
    humanresources_router, 
    product_router, 
    purchasing_router, 
    sales_router
)

app = FastAPI(
    title="Data Platform API",
    description="API para orquestração e ingestão de dados do Data Platform (Silver/Gold Layers)",
    version="1.0.0"
)

# Registrando os roteadores de domínio (3-Layer Arch)
app.include_router(warehouse_router.router)
app.include_router(marketing_router.router)
app.include_router(person_router.router)
app.include_router(humanresources_router.router)
app.include_router(product_router.router)
app.include_router(purchasing_router.router)
app.include_router(sales_router.router)

@app.get("/")
async def root():
    return {
        "message": "Data Platform API is running",
        "status": "healthy",
        "available_domains": ["warehouse", "marketing", "person", "humanresources", "product", "purchasing", "sales"]
    }
