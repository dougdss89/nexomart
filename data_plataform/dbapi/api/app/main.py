from api.routers import (
    humanresources_router,
    marketing_router,
    person_router,
    product_router,
    purchasing_router,
    sales_router,
    warehouse_router,
)
from fastapi import FastAPI

app = FastAPI(
    title="Data Platform API",
    description="API for orchestration and data ingestion of the Data Platform (Silver/Gold Layers)",
    version="1.0.0",
)

# Registering the domain routers (3-Layer Arch)
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
        "available_domains": [
            "warehouse",
            "marketing",
            "person",
            "humanresources",
            "product",
            "purchasing",
            "sales",
        ],
    }
