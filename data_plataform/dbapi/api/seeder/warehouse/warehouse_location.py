import logging
import asyncio
import json
import random
import uuid
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import select, func

# Mock setup for DB session
from api.database import AsyncSessionLocal
from api.models.warehouse.warehouselocation import WarehouseLocationModel
from api.schemas.warehouse.warehouse_wrhlocation import (
    WarehouseLocationSchema,
    LocationCapabilities,
    GeoLocation,
    TemperatureRange,
    WarehouseType
)
from api.services.warehouse.warehouse_location_service import WarehouseLocationService

from api.samples.warehouse_location import WAREHOUSE_LOCATION_SAMPLES
from api.seeder.chaos import apply_warehouse_location_chaos
from api.utils.cli_common import build_parser, add_volume_args, resolve_volume_args, stamp_dimension_history

logger = logging.getLogger(__name__)

SCHEMA_NAME = "warehouse"
TABLE_NAME = "warehouse.warehouselocation"

class WarehouseLocationSeeder:
    """
    Realistic Warehouse Location Generator.
    """
    
    LOCATIONS_SAMPLES = WAREHOUSE_LOCATION_SAMPLES

    @classmethod
    def generate_batch(cls, count: int, chaos_rate: float = 0.05, apply_chaos: bool = True, start_id: int = 1) -> List[Dict[str, Any]]:
        batch = []
        for offset in range(count):
            i = start_id + offset - 1
            sample = random.choice(cls.LOCATIONS_SAMPLES)
            warehouse_type = random.choice(list(WarehouseType))
            
            # Simulated Temperature range based on type
            temp_range = None
            if warehouse_type == WarehouseType.COLD_STORAGE:
                temp_range = TemperatureRange(min_temp=-25.0, max_temp=-5.0)
            elif warehouse_type == WarehouseType.HOT_STORAGE:
                temp_range = TemperatureRange(min_temp=25.0, max_temp=45.0)
            
            capabilities = LocationCapabilities(
                warehouse_type=warehouse_type,
                temperature_range=temp_range,
                is_secure_area=random.choice([True, False]),
                aisle=f"A-{random.randint(1, 50)}",
                shelf=f"S-{random.randint(1, 10)}",
                bin=f"B-{random.randint(1, 100)}"
            )
            
            geo = GeoLocation(
                address=f"Route {random.randint(100, 999)}, Industrial Zone",
                address_line=f"Building {random.choice(['A', 'B', 'C', 'D'])}",
                warehouse_number=str(random.randint(1, 500)),
                country_id=random.randint(1, 10),
                country_name=sample["country"],
                state_province_id=random.randint(1, 50),
                state_province_name=sample["state"],
                city_id=random.randint(1, 1000),
                city_name=sample["city"],
                postal_code=f"{random.randint(10000, 99999)}-{random.randint(100, 999)}"
            )
            
            schema_data = WarehouseLocationSchema(
                warehouselocation_id=i + 1,
                warehouse_id=random.randint(1, 1000), # Simulated external ID
                name=f"{sample['name']} Unit {i+1}",
                cost_rate=Decimal(random.uniform(5.50, 45.00)).quantize(Decimal("0.01")),
                availability=Decimal(random.uniform(85.0, 100.0)).quantize(Decimal("0.01")),
                capabilities=capabilities,
                geo_location=geo
            )
            
            # WarehouseLocationModel uses different column names than the Pydantic schema
            # (warehousename/costrate, without warehouse_id — that's just a simulated external ID).
            data_dict = schema_data.model_dump()
            data_dict["warehousename"] = data_dict.pop("name")
            data_dict["costrate"] = data_dict.pop("cost_rate")
            data_dict.pop("warehouse_id", None)
            batch.append(data_dict)

        if apply_chaos:
            batch = apply_warehouse_location_chaos(batch, chaos_rate=chaos_rate)

        return batch

async def run_seeder(count: int, chaos_rate: float = 0.05, batch_size: int = 10_000, apply_chaos: bool = True):
    print(f"🚀 Starting Warehouse Location Seeder: {count:,} records | chaos={chaos_rate:.2%}")

    seeder = WarehouseLocationSeeder()

    async with AsyncSessionLocal() as db:
        max_id = await db.scalar(select(func.max(WarehouseLocationModel.warehouselocation_id))) or 0
        next_id = max_id + 1

        total_inserted = 0
        while total_inserted < count:
            current_batch_size = min(batch_size, count - total_inserted)
            data = seeder.generate_batch(current_batch_size, chaos_rate=chaos_rate, apply_chaos=apply_chaos, start_id=next_id)
            next_id += current_batch_size
            stamp_dimension_history(data)  # backdate created_at so the landing spans 2020->now
            try:
                success = await WarehouseLocationService.process_bulk_integration(db, data)
                if success:
                    total_inserted += len(data)
            except Exception as e:
                await db.rollback()
                print(f"❌ Error during load: {e}")
                raise RuntimeError(f"Warehouse location seeding aborted ({total_inserted:,}/{count:,} inserted): {e}") from e

        print(f"✅ Success! {total_inserted:,} records inserted into the database via Bulk Insert.")

if __name__ == "__main__":
    parser = build_parser(f"Dynamic seeder for {TABLE_NAME}")
    add_volume_args(parser, schema=SCHEMA_NAME, table=TABLE_NAME)
    args = parser.parse_args()
    count, chaos_rate, apply_chaos, batch_size = resolve_volume_args(args, schema=SCHEMA_NAME, table=TABLE_NAME)

    asyncio.run(run_seeder(count=count, chaos_rate=chaos_rate, batch_size=batch_size, apply_chaos=apply_chaos))
