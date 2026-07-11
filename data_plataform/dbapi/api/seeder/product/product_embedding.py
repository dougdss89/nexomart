import uuid
import hashlib
import random
from datetime import datetime
from api.seeder.chaos import apply_embedding_chaos

def get_product_embedding_seeders(products: list[dict]) -> list[dict]:
    """
    Gera um lote de embeddings baseado na lista de produtos gerados pelo seeder de produtos.
    Como é um mock inicial para futuros testes de PGVector, os vetores serão arrays
    com valores aleatórios entre -1.0 e 1.0, simulando o output de modelos reais.
    """
    batch = []
    
    for product in products:
        
        product_id = product["product_id"]
        product_name = product["product_name"]
        
        # 1. Simulação do source_data_hash (Controle de Cache/ETL)
        # Na vida real, seria o hash do texto + metadados concatenados a serem embedados
        text_to_embed = f"{product_name} {product['productdetails'].get('brand', '')} {product['productdetails'].get('material', '')}"
        source_hash = hashlib.sha256(text_to_embed.encode('utf-8')).hexdigest()
        
        # 2. Geração Randômica de Vetores (PGVector)
        # OpenAI text-embedding-3-small tem 1536 dimensões
        desc_vector = [round(random.uniform(-1.0, 1.0), 6) for _ in range(1536)]
        
        # Clip ViT-Base-Patch32 tem 512 dimensões
        img_vector = [round(random.uniform(-1.0, 1.0), 6) for _ in range(512)]
        
        # 3. Metadados de rastreabilidade (Conforme AIModelMetadata)
        ai_metadata = {
            "text_model": "openai-text-embedding-3-small",
            "image_model": "openai-clip-vit-base-patch32",
            "generation_method": "mock_seeder_batch"
        }
        
        # 4. Montagem do payload
        # O sqlalchemy pgvector e o jsonb lidarão com as conversões internamente na camada service
        batch.append({
            "product_id": product_id,
            "description_vector": desc_vector,
            "image_vector": img_vector,
            "source_data_hash": source_hash,
            "ai_metadata": ai_metadata,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        })

    return apply_embedding_chaos(batch)

# --- TESTANDO NO TERMINAL ---
if __name__ == "__main__":
    # Importar mock para testar
    from api.seeder.product.product import get_product_seeders
    
    print("Gerando produtos mock para base do teste...")
    produtos_mock = get_product_seeders(quantidade=2)
    
    print("Gerando embeddings mockados...")
    embeddings_mock = get_product_embedding_seeders(produtos_mock)
    
    print(f"Total de embeddings gerados: {len(embeddings_mock)}")
    print("-" * 50)
    
    # Ao imprimir, não vamos mostrar os arrays gigantes 
    for emb in embeddings_mock:
        emb_display = emb.copy()
        
        if emb_display["description_vector"]:
            emb_display["description_vector"] = f"[Array with {len(emb_display['description_vector'])} floats]"
            
        if emb_display["image_vector"]:
            emb_display["image_vector"] = f"[Array with {len(emb_display['image_vector'])} floats]"
            
        import json
        print(json.dumps(emb_display, indent=2, default=str))
