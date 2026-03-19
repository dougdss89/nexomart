import uuid
import random
from datetime import datetime, timedelta, date
from faker import Faker
from typing import Dict

# IMPORTANDO OS DADOS REAIS DA NOSSA API
from api.samples.job_roles import CARGOS_OPERACAO
from api.seeder.humanresources.humanresources_department import get_department_seeders
from api.samples.country import PAISES_OPERACAO
from api.samples.state_province import ESTADOS_OPERACAO

fake = Faker('pt_BR')

def get_job_role_map() -> Dict[str, int]:
    return {cargo[0]: idx for idx, cargo in enumerate(CARGOS_OPERACAO, start=1)}

def get_department_map() -> Dict[str, int]:
    depts = get_department_seeders()
    return {str(dept["department_name"].value if hasattr(dept["department_name"], 'value') else dept["department_name"]): dept["department_id"] for dept in depts}

def get_geography_map() -> tuple[list, dict]:
    """Retorna a lista de países disponíveis e um mapa de seus respectivos estados."""
    paises_dict = {p[0]: {"region": p[1], "currency": p[2], "db_id": idx} for idx, p in enumerate(PAISES_OPERACAO, start=1)}
    
    # Mapeia Estados para IDs (Lógica realocada do seeder de Person)
    estados_map = {}
    estado_id = 1
    for country_code, estados in ESTADOS_OPERACAO.items():
        if country_code in paises_dict: # Somente se o País base estiver ativado
            for est in estados:
                estados_map[est[0]] = {"db_id": estado_id, "country_code": country_code}
                estado_id += 1
                
    return paises_dict, estados_map
    
def generate_role_attributes(funcao: str, is_manager: bool, seniorityLevel: str = "MID") -> dict:
    """Gera o JSONB Polimórfico validado pelo Pydantic"""
    
    # 1. Sales Manager Role Attributes (Bonus driven)
    if funcao == "VENDAS" and is_manager:
        return {
            "annual_bonus_target_pct": round(random.uniform(10.0, 30.0), 2),
            "quota_achievement_multiplier": round(random.uniform(1.2, 2.5), 2),
            "managed_territory_codes": random.sample(["SP", "RJ", "MG", "SUL", "NORDESTE"], k=random.randint(2, 5))
        }
        
    # 2. Sales Leaf Role Attributes (Commission driven by Seniority)
    elif funcao == "VENDAS" and not is_manager:
        if seniorityLevel == "JUNIOR": # Menor base, alta comisssao % (fome)
            comission = round(random.uniform(3.0, 5.0), 2)
            limit = random.randint(30, 80)
        elif seniorityLevel == "SENIOR": # Maior base fixa, menor comissao % porem ticket infinito
            comission = round(random.uniform(0.5, 1.5), 2)
            limit = 0 # Infinito, Schema nullable na nova versao
        else:
            comission = round(random.uniform(1.5, 2.5), 2)
            limit = random.randint(80, 150)
            
        return {
            "sales_quota_quaterly": round(random.uniform(15000.0, 50000.0), 2),
            "territory_codes": random.sample(["SP", "RJ", "MG", "SUL", "NORDESTE"], k=random.randint(1, 3)),
            "comission_pct": comission,
            "portfolio_size_limit": None if limit == 0 else limit
        }
        
    # 3. Buyers
    elif funcao == "COMPRAS":
        return {
            "managed_categories": random.sample(["ELECTRONICS", "CLOTHING", "FOOTWEAR", "HARDWARE"], k=random.randint(1, 3)),
            "approval_limit_value": round(random.uniform(50000.0, 200000.0), 2),
            "warehouse_sections": ["A1", "B2"],
            "supplier_rating_min": round(random.uniform(3.0, 4.0), 1),
            "supplier_rating_max": 5.0
        }
        
    # 4. Field Marketing
    elif funcao == "MARKETING_CAMPO":
        return {
            "device_id": f"MKT-IPAD-{random.randint(100, 999)}",
            "survey_zones": random.sample(["ZONA SUL", "SHOPPING", "CENTRO"], k=2),
            "shift_schedule": "09:00-18:00",
            "daily_survey_target": random.randint(10, 50)
        }
        
    return {} 


def get_employee_seeders() -> list[dict]:
    """Gera a carga de Funcionários injetando Geografia, Câmbio Monetário e o JSONB Polymórfico de Vendas"""
    batch = []
    employee_id_counter = 1
    
    # Foreign Keys Maps
    job_map = get_job_role_map()
    dept_map = get_department_map()
    paises, estados = get_geography_map()
    
    def criar_funcionario(job_title: str, department_string: str, funcao_pydantic: str = "GERAL", is_manager=False, seniority="MID", manager_id: int = None, force_country: str = None) -> dict:
        nonlocal employee_id_counter
        
        # Sorteia País
        assigned_country_code = force_country if force_country else random.choices(["BR", "US", "PT", "DE"], weights=[0.6, 0.2, 0.1, 0.1])[0]
        country_info = paises[assigned_country_code]
        
        # Acha estados válidos pra este país (Fallback se não houver mapeamento de estados praquele pais)
        valid_states = [v for k, v in estados.items() if v["country_code"] == assigned_country_code]
        assigned_state = random.choice(valid_states) if valid_states else {"db_id": 1} # Fallback Seguro (ID 1 de Estado)
        
        primeiro_nome = fake.first_name()
        sobrenome = fake.last_name()
        email = f"{primeiro_nome.lower()}.{sobrenome.lower()}@empresa.com"
        cpf_numeros = fake.cpf().replace(".", "").replace("-", "")
        data_contratacao = (datetime.now() - timedelta(days=random.randint(100, 3000))).date()
        
        # Conversão Monetária Básica (SCD Câmbio)
        cargo_info = next(c for c in CARGOS_OPERACAO if c[0] == job_title)
        salario_min, salario_max = cargo_info[1], cargo_info[2]
        base_salary_brl = round(random.uniform(salario_min, salario_max), 2)
        
        if country_info["currency"] == "USD":
            final_salary = round(base_salary_brl / 5.20, 2)
        elif country_info["currency"] == "EUR":
            final_salary = round(base_salary_brl / 5.50, 2)
        else:
            final_salary = base_salary_brl

        func_dict = {
            "employee_id": employee_id_counter,
            "first_name": primeiro_nome,
            "last_name": sobrenome,
            "nationalidnumber": cpf_numeros, 
            "loginid": email, 
            "birthdate": (datetime.now() - timedelta(days=random.randint(22*365, 60*365))).date(),
            "email": email,
            
            "hire_date": data_contratacao,
            "end_date": None, 
            
            "jobrole_id": job_map[job_title],
            "department_id": dept_map[department_string],
            "manager_id": manager_id,
            
            # --- INCORPORAÇÃO GEOGRÁFICA ---
            "country_id": country_info["db_id"],
            "state_province_id": assigned_state["db_id"],
            "local_currency": country_info["currency"],
            
            # JSONB
            "role_attributes": generate_role_attributes(funcao_pydantic, is_manager, seniority),
            
            "current_salary": final_salary,
            
            "effective_start_date": datetime.combine(data_contratacao, datetime.min.time()),
            "effective_end_date": None,
            "is_current": True,
            
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        }
        
        employee_id_counter += 1
        return func_dict

    # --- MONTAGEM DA HIERARQUIA CORPORATIVA ---
    ceo = criar_funcionario("Chief Executive Officer (CEO)", "executive", "GERAL", True, manager_id=None, force_country="US") # CEO mora nos EUA
    batch.append(ceo)
    
    cfo = criar_funcionario("Chief Financial Officer (CFO)", "finance", "GERAL", True, manager_id=ceo["employee_id"])
    cmo = criar_funcionario("Chief Marketing Officer (CMO)", "marketing", "GERAL", True, manager_id=ceo["employee_id"])
    diretor_vendas = criar_funcionario("Diretor(a) de Vendas", "sales", "VENDAS", True, manager_id=ceo["employee_id"]) # Sales Manager (Board)
    cto = criar_funcionario("Chief Technology Officer (CTO)", "engineering", "GERAL", True, manager_id=ceo["employee_id"], force_country="US")
    
    batch.extend([cfo, cmo, diretor_vendas, cto])

    gerente_dados = criar_funcionario("Engenheiro(a) de Dados Sênior", "engineering", "GERAL", True, manager_id=cto["employee_id"])
    gerente_vendas = criar_funcionario("Gerente de Vendas Nacionais", "sales", "VENDAS", True, manager_id=diretor_vendas["employee_id"]) # Sales Manager logic
    gerente_compras = criar_funcionario("Gerente de Logística e Supply Chain", "purchasing", "COMPRAS", True, manager_id=cfo["employee_id"]) 
    
    batch.extend([gerente_dados, gerente_vendas, gerente_compras])

    # Chão de Fábrica Vendas (Sales Leaf - Comissao por Senioridade)
    for _ in range(2):
        sdr_junior = criar_funcionario("SDR (Sales Development Representative)", "sales", "VENDAS", is_manager=False, seniority="JUNIOR", manager_id=gerente_vendas["employee_id"])
        batch.append(sdr_junior)
        
    for _ in range(2):
        sdr_senior = criar_funcionario("Executivo(a) de Contas Sênior (Account Executive)", "sales", "VENDAS", is_manager=False, seniority="SENIOR", manager_id=gerente_vendas["employee_id"])
        batch.append(sdr_senior)

    # Chão de Fábrica Restante
    for _ in range(3):
        comprador = criar_funcionario("Comprador(a) Sênior (Procurement)", "purchasing", "COMPRAS", is_manager=False, manager_id=gerente_compras["employee_id"])
        batch.append(comprador)
        
    for _ in range(2):
        mkt = criar_funcionario("Growth Hacker / Especialista em Growth", "marketing", "MARKETING_CAMPO", is_manager=False, manager_id=cmo["employee_id"])
        batch.append(mkt)
        
    for _ in range(3):
        # Alocar engenheiros aleatoriamente EUA ou BR
        eng_pleno = criar_funcionario("Engenheiro(a) de Dados Pleno", "engineering", "GERAL", is_manager=False, manager_id=gerente_dados["employee_id"], force_country=random.choice(["BR", "US"]))
        batch.append(eng_pleno)

    return batch

# --- TESTANDO NO TERMINAL ---
if __name__ == "__main__":
    funcionarios_mock = get_employee_seeders()
    
    print(f"Total de funcionários gerados (Internacional/Complex Sales): {len(funcionarios_mock)}")
    print("-" * 50)
    import json
    
    # Vamos validar os outputs (Forex USD vs BRL) e SalesManager vs SalesSalesRep
    jobmap = get_job_role_map()
    ceo_us = next(f for f in funcionarios_mock if f["jobrole_id"] == jobmap["Chief Executive Officer (CEO)"])
    br_sales_manager = next(f for f in funcionarios_mock if f["jobrole_id"] == jobmap["Gerente de Vendas Nacionais"]) 
    br_sales_junior = next(f for f in funcionarios_mock if f["jobrole_id"] == jobmap["SDR (Sales Development Representative)"]) 
    br_sales_senior = next(f for f in funcionarios_mock if f["jobrole_id"] == jobmap["Executivo(a) de Contas Sênior (Account Executive)"]) 
    
    print("\n[FOREX] --- CEO (Morador dos EUA ganhando em USD) ---")
    print(f"Moeda: {ceo_us['local_currency']} | Salario (USD): $ {ceo_us['current_salary']} | País ID: {ceo_us['country_id']}")
    
    print("\n[SALES POLY] --- GERENTE DE VENDAS (SalesManager Attributes - Bonus) ---")
    print(json.dumps(br_sales_manager["role_attributes"], indent=2, default=str))
    
    print("\n[SALES POLY] --- VENDEDOR JUNIOR (SalesRole Attributes - High Commission) ---")
    print(json.dumps(br_sales_junior["role_attributes"], indent=2, default=str))

    print("\n[SALES POLY] --- VENDEDOR SÊNIOR (SalesRole Attributes - Low Commission, Infinite Quota) ---")
    print(json.dumps(br_sales_senior["role_attributes"], indent=2, default=str))
