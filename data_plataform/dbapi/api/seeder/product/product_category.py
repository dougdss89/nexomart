import uuid
from datetime import datetime, timedelta
import random
from api.schemas.product.production_product_category import ProductCategory

def get_category_seeders() -> list[dict]:
    
    batch = []
    dias_passados = random.randint(30, 365)
    data_simulada = datetime.now() - timedelta(days=dias_passados)

    for index, category in enumerate(ProductCategory, start=1):
        
        batch.append(
            {
                "category_id": index,
                "category_name": category.value,

                "created_at": data_simulada,
                "updated_at": data_simulada,
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
        )
    
    return batch