import logging
import argparse
import asyncio
import json
import random
import uuid
from decimal import Decimal
from datetime import datetime
from typing import List, Dict, Any

# Mock setup for DB session
from api.database import AsyncSessionLocal
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

logger = logging.getLogger(__name__)

class WarehouseLocationSeeder:
    """ 
    Gerador Realístico de Localizações de Armazém.
    """
    
    LOCATIONS_SAMPLES = WAREHOUSE_LOCATION_SAMPLES

    @classmethod
    def generate_batch(cls, count: int) -> List[Dict[str, Any]]:
        batch = []
        for i in range(count):
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
                warehouse_id=random.randint(1, 1000), # Simulação de ID externo
                name=f"{sample['name']} Unit {i+1}",
                cost_rate=Decimal(random.uniform(5.50, 45.00)).quantize(Decimal("0.01")),
                availability=Decimal(random.uniform(85.0, 100.0)).quantize(Decimal("0.01")),
                capabilities=capabilities,
                geo_location=geo
            )
            
            data_dict = schema_data.model_dump()
            batch.append(data_dict)

        return apply_warehouse_location_chaos(batch)

async def run_seeder(count: int):
    print(f"🚀 Iniciando Seeder de Warehouse Location: {count} registros...")
    
    seeder = WarehouseLocationSeeder()
    data = seeder.generate_batch(count)
    
    async with AsyncSessionLocal() as db:
        try:
            success = await WarehouseLocationService.process_bulk_integration(db, data)
            if success:
                print(f"✅ Sucesso! {len(data)} registros inseridos no banco via Bulk Insert.")
                # Auditoria visual
                print("Exemplo de carga:")
                print(json.dumps(data[0], indent=2, default=str))
        except Exception as e:
            print(f"❌ Erro na carga: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder de Warehouse Location para Airflow.")
    parser.add_argument("--count", type=int, default=10, help="Quantidade de registros a gerar")
    args = parser.parse_args()
    
    asyncio.run(run_seeder(args.count))
