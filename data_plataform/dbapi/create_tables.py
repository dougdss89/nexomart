"""
Setup script: creates schemas and all tables in nexomarket_db.
Usage: uv run python dbapi/create_tables.py
"""
import asyncio
import sys
sys.path.insert(0, "dbapi")

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

from api.config.base import Base

# Importar todos os models para registrar no metadata
from api.models.product.productcategory import ProductCategoryModel
from api.models.product.productmodel import ProductModel
from api.models.product.product import Product
from api.models.product.product_embedding import ProductEmbeddingModel
from api.models.person.country_region import CountryRegionModel
from api.models.person.state_province import StateProvinceModel
from api.models.person.customer import CustomerModel
from api.models.person.customer_email import CustomerEmailModel
from api.models.person.seller import SellerModel
from api.models.sales.saleschannel import SalesChannelModel
from api.models.sales.specialoffer import SpecialOfferModel
from api.models.sales.specialofferproduct import SpecialOfferProductModel
from api.models.sales.salesorder import SalesOrderModel
from api.models.sales.returnorder import SalesReturnOrderModel
from api.models.sales.ordereventlog import SalesOrderEventLogModel
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.models.warehouse.transactiontype import WarehouseTransactionType
from api.models.warehouse.productinventory import WarehouseProductInventoryModel
from api.models.warehouse.transactionhistory import TransactionHistoryModel
from api.models.humanresources.department import HumanResourcesDepartmentModel
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.marketing.marketing_channel import MarketingChannelModel
from api.models.marketing.marketing_campaign import MarketingCampaignModel
from api.models.marketing.marketing_adgroup import MarketingAdGroupModel
from api.models.marketing.marketing_advertise import MarketingAdvertiseModel
from api.models.marketing.marketing_dailyperformance import MarketingDailyPerformanceModel
from api.models.purchasing.vendor import PurchaseVendorModel
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.models.purchasing.purchaseorderdetail import PurchaseOrderDetailModel
from api.models.purchasing.purchaseorderaudit import PurchaseOrderAuditModel

DATABASE_URL = "postgresql+asyncpg://admin:admin@localhost:5432/nexomarket_db"

SCHEMAS = [
    "product", "person", "sales", "warehouse",
    "humanresources", "marketing", "purchasing"
]

async def setup():
    engine = create_async_engine(DATABASE_URL, echo=False)

    async with engine.begin() as conn:
        # 1. Create schemas
        for schema in SCHEMAS:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        print(f"✅ {len(SCHEMAS)} schemas created")

        # 2. Create extensions
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\""))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        print("✅ Extensions: uuid-ossp, vector")

        # 3. Create all tables
        await conn.run_sync(Base.metadata.create_all)
        print(f"✅ Tables created")

    # 4. Count created tables
    async with engine.connect() as conn:
        result = await conn.execute(text("""
            SELECT schemaname, COUNT(*) as n
            FROM pg_tables
            WHERE schemaname = ANY(:schemas)
            GROUP BY schemaname
            ORDER BY schemaname
        """), {"schemas": SCHEMAS})
        rows = result.fetchall()
        total = sum(r.n for r in rows)
        for r in rows:
            print(f"   {r.schemaname}: {r.n} tables")
        print(f"\n✅ Total: {total} tables across {len(rows)} schemas")

    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(setup())
