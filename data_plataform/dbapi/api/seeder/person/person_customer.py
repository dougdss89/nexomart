import uuid
import random
from datetime import datetime
from typing import List, Dict, Any
from api.samples.customer import generate_mock_customer

# Importar as coleções brutas para deixar a FK Dinâmica
from api.samples.country import PAISES_OPERACAO
from api.samples.state_province import ESTADOS_OPERACAO

# Contagem dinâmica de IDs
DYNAMIC_MAX_COUNTRIES = len(PAISES_OPERACAO)

# Os estados estão em um dicionário de listas. Soma-se o tamanho de cada lista.
DYNAMIC_MAX_STATES = sum(len(estados) for estados in ESTADOS_OPERACAO.values())

def get_customer_seeders(quantidade: int = 100) -> List[Dict[str, Any]]:
    """
    Gera uma lista de dicionários contendo dados para a tabela Customer.
    Faz a orquestração do gerador de samples, adicionando as chaves estrangeiras
    e metadados nativos do banco de dados.
    """
    try:
        batch = []
        
        for i in range(1, quantidade + 1):
            
            # Busca payload falso base
            sample_data = generate_mock_customer()
            
            # Atribui dependências estruturais DB (Chaves estrangeiras e metadados)
            # Importante: Como é um Mock, estamos sorteando o FK ID sem validar no DB.
            db_data = {
                "customer_name": sample_data["customer_name"],
                "last_name": sample_data["last_name"],
                "social_number": sample_data["social_number"],
                "birthdate": sample_data["birthdate"],
                "customer_type": sample_data["customer_type"],
                "credit_limit": sample_data["credit_limit"],
                "demographics": sample_data["demographics"],
                
                # Foreign Keys dinâmicas baseadas no comprimento total dos samples de Paises e Estados
                "country_id": random.randint(1, DYNAMIC_MAX_COUNTRIES) if DYNAMIC_MAX_COUNTRIES > 0 else 1,
                "state_province_id": random.randint(1, DYNAMIC_MAX_STATES) if DYNAMIC_MAX_STATES > 0 else 1,
                
                # Metadados de Auditoria
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "is_active": True,
                "row_guid": str(uuid.uuid4())
            }
            
            batch.append(db_data)

        return batch
        
    except Exception as e:
        print(f"Erro ao gerar seeders de clientes: {e}")
        return []
        
if __name__ == "__main__":
    clientes_mock = get_customer_seeders(quantidade=10)
    
    print(f"Total de clientes gerados: {len(clientes_mock)}")
    print("-" * 50)
    
    import json
    print(json.dumps(clientes_mock, indent=2, default=str))