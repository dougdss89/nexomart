import random
import uuid
from datetime import datetime, timedelta
from api.samples.product import generate_randomized_catalog
from api.schemas.product.production_product_category import ProductCategory
from api.schemas.product.production_product_model import ProductModel
from api.seeder.chaos import apply_product_chaos

def get_product_seeders(quantidade: int = 100) -> list[dict]:
    """
    Gera um lote massivo de produtos realistas baseados na loja de e-commerce simulada.
    """
    batch = []
    
    # 1. Obter o catálogo virtual sorteado e diversificado ("Combinatorial Seeder")
    catalog_instances = generate_randomized_catalog(quantidade)
    
    # Mapeamento auxiliar para resolver a Category_ID e Model_ID
    # (Como o seeder de Categoria/Modelos simplesmente converte os Enumeradores em IDs na ordem de definição, 
    # podemos pegar o index correspondente nas próprias classes Enums originais)
    category_list = list(ProductCategory)
    model_list = list(ProductModel)

    for product_id, item in enumerate(catalog_instances, start=1):
        
        # Encontra o ID correto da Categoria e do Modelo baseado no que o Seeder de Categoria inseriu no DB
        try:
            category_id = category_list.index(item["category"]) + 1
            model_id = model_list.index(item["model"]) + 1
        except ValueError:
            # Fallback seguro
            category_id = 1
            model_id = 1
            
        # Ciclo de venda: a maioria ainda esta em venda (sell_end_date=None)
        sell_start_date = datetime.now() - timedelta(days=random.randint(30, 730))

        # 2. Montagem do Dicionário (Mapeia perfeitamente para o SQLAlchemy e JSONB)
        batch.append({
            "product_id": product_id,
            "product_name": item["product_name"][:100],
            "model_id": model_id,
            "category_id": category_id,

            # O SQLAlchemy converte esse dicionário interno em JSONB automaticamente
            "productdetails": item["details"],

            "sell_start_date": sell_start_date,
            "sell_end_date": None,

            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        })

    return apply_product_chaos(batch)
