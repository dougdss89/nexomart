import uuid
import random
from datetime import datetime, timedelta

from api.samples.job_roles import CARGOS_OPERACAO

def get_job_role_seeders() -> list[dict]:

    """
    Gera seeders de cargos (job roles) com base salarial e histórico dos profissionais.
    """
    
    batch = []

    job_role_id_counter = 1

    data_hoje = datetime.now()
    
    for titulo, salario_minimo, salario_maximo in CARGOS_OPERACAO:
        
        reajuste = random.choice([True, False, False])

        if reajuste:
            #gera registro antigo ou inativo
            #vigencia de 3 anos ou 1 ano
            #salario minimo e maximo com reajuste de 20%

            start_antigo = data_hoje - timedelta(days=random.randint(700, 1500))
            end_antigo = start_antigo + timedelta(days=random.randint(180, 600))

            # Ajuste Lógico: O antigo salário era menor que o atual,
            # variando de 10 a 25% de diferença.
            fator_crescimento = random.uniform(0.75, 0.90)
            salario_minimo_antigo = round(salario_minimo * fator_crescimento, 2)
            salario_maximo_antigo = round(salario_maximo * fator_crescimento, 2)

            batch.append({
                "jobrole_id": job_role_id_counter,
                "job_title": titulo,
                "min_salary": salario_minimo_antigo,
                "max_salary": salario_maximo_antigo,
                "effective_start_date": start_antigo,
                "effective_end_date": end_antigo,
                "is_current": False,

                "created_at": data_hoje,
                "updated_at": data_hoje,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })

            job_role_id_counter += 1

            start_atual = end_antigo + timedelta(seconds=1)

        else:
            start_atual = data_hoje - timedelta(days = random.randint(500, 1000))


        #registro atual
        batch.append({
            "jobrole_id": job_role_id_counter,
            "job_title": titulo,
            "min_salary": salario_minimo,
            "max_salary": salario_maximo,
            "effective_start_date": start_atual,
            "effective_end_date": None,
            "is_current": True,

            "created_at": data_hoje,
            "updated_at": data_hoje,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

        job_role_id_counter += 1    

    return batch

if __name__ == "__main__":
    cargos_mock = get_job_role_seeders()
    
    print(f"Total de registros de cargo gerados (incluindo histórico): {len(cargos_mock)}")
    print("-" * 50)
    
    import json
    # Exibe os dois primeiros registros para você validar a matemática das datas
    print(json.dumps(cargos_mock[:10], indent=2, default=str))