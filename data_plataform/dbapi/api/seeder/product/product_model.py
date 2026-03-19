import uuid
from datetime import datetime, timedelta
import random
from api.schemas.product.production_product_model import ProductModel

def get_model_seeders() -> list[dict]:

    batch = []
    dias_passados = random.randint(30, 365)
    data_simulada = datetime.now() - timedelta(days=dias_passados)

    for index, model in enumerate(ProductModel, start=1):
        
        batch.append(
            {
                "product_model_id": index,
                "model_name": model,
                "created_at": data_simulada,
                "updated_at": data_simulada,
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
        )
    
    return batch
