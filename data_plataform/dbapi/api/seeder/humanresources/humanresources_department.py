import uuid
from datetime import datetime

# Ajuste os imports para o caminho correto do seu projeto
from api.schemas.humanresources.humanresources_department import (
    HRDepartment, 
    DEPARTMENT_TO_GROUP_MAP
)

from api.samples.hr_department_organization import ORGANOGRAMA

def get_department_seeders() -> list[dict]:
    """
    Gera seeders de departamentos baseados na estrutura hierárquica.
    """
    
    batch = []
    department_id_counter = 1
    
    # Dicionário para mapear nomes de de3partamentos para seus IDs
    # Isso é necessário para criar os relacionamentos (parent_department_id)
    department_map = {}
    
    data_hoje = datetime.now()
    
    for dept_name, parent_name in ORGANOGRAMA:
        
        # 1. Obter o ID do pai
        parent_id = None
        if parent_name:
            # Se o pai ainda não foi processado (improvável na ordem do ORGANOGRAMA, mas seguro checar)
            if parent_name not in department_map:
                # Fallback: Se o pai não estiver no mapa, assumimos que ele é um departamento de nível superior
                # que já deveria ter sido criado. Para este exemplo, vamos assumir que a ordem do ORGANOGRAMA
                # garante que o pai venha antes.
                pass 
            
            parent_id = department_map.get(parent_name)

        # 2. Criar o registro do departamento atual
        # O grupo é determinado automaticamente pelo validador do Pydantic
        
        # Nota: Para simplificar o seeder, vamos criar apenas registros "atuais" (is_active=True, effective_end_date=None)
        # Se você quiser histórico, precisaria de uma lógica mais complexa aqui também.
        
        batch.append({
            "department_id": department_id_counter,
            "department_name": dept_name,
            "parent_department_id": parent_id,
            "manager_id": None, # Você pode popular com IDs de funcionários depois, se quiser
            
            "created_at": data_hoje,
            "updated_at": data_hoje,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
        
        # 3. Atualizar o mapa para uso futuro
        department_map[dept_name] = department_id_counter
        
        department_id_counter += 1
        
    return batch

if __name__ == "__main__":
    depts = get_department_seeders()
    print(f"Total de departamentos gerados: {len(depts)}")
    import json
    print(json.dumps(depts, indent=2, default=str))
