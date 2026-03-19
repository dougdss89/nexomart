import uuid
import pycountry
from datetime import datetime, timedelta
from typing import List, Dict, Any
from api.samples.state_province import ESTADOS_OPERACAO
from api.samples.country import PAISES_OPERACAO

def get_state_province_seeders() -> List[Dict[str, Any]]:
    
    """ Gera a carga de estados vinculados dinamicamente aos paises"""

    batch = []

    # cria um mapa entre codigo do pais e id
    country_id_map = {code: index for index, (code, _) in enumerate(PAISES_OPERACAO, start=1)}

    data_setup = datetime.now()

    state_id_counter = 1

    #le o dicionario de estados
    for country_code, lista_estados in ESTADOS_OPERACAO.items():
        
        current_country_id = country_id_map.get(country_code)

        if not current_country_id:
            continue
            
        for state_code, state_name in lista_estados:

            batch.append({
                "state_province_id": state_id_counter,
                "state_province_name": state_name,
                "state_province_code": state_code,
                "country_id": current_country_id,

                "created_at": data_setup,
                "updated_at": data_setup,
                "is_active": True,
                "row_guid": str(uuid.uuid4())
            })
            
            
    return batch
