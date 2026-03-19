import uuid 
import pycountry
from datetime import datetime, timedelta
from typing import List, Dict, Any
from api.samples.country import PAISES_OPERACAO

def get_country_seeders() -> List[Dict[str, Any]]:
    
    """
    Gera uma lista de dicionários contendo dados para a tabela CountryRegion.
    """
    try:
        batch = []
        
        data_setup = datetime.now() - timedelta(days=730)

        for index, (code, region) in enumerate(PAISES_OPERACAO, start=1):

            #Pycountry busca pelo código Alpha-2
            country = pycountry.countries.get(alpha_2=code)

            if country:
                country_name = country.name
            else:
                country_name = None

            batch.append({
                "country_id": index,
                "country_code": code,
                "country_region": region,
                "country_name": country_name,

                "created_at": data_setup,
                "updated_at": data_setup,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })

        return batch

    except Exception as e:
        print(f"Erro ao gerar seeders: {e}")
        return []
