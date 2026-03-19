import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

# Import do Schema Pydantic para validação on-the-fly
from api.schemas.purchasing.purchasing_vendor import Vendor
import api.samples.purchase_payment_certs as purchase_payment_certs

fake = Faker('pt_BR')

def get_vendor_details(num_vendors: int) -> list[dict]:
    """ Gera catálogo de fornecedores com Histórico (SCD2) e Engenharia do Caos (Data Quality) """
    
    batch = []
    vendor_counter_id = 1

    for _ in range(num_vendors):
        nome_empresa = fake.company()
        # Formatação Padrão Limpa
        cnpj = fake.cnpj() 
        
        # --- ENGENHARIA DO CAOS (Data Quality) ---
        # 10% de chance de CNPJ sujo (sem formatação)
        if random.random() < 0.10:
            cnpj = cnpj.replace(".", "").replace("-", "").replace("/", "")
            
        # Contato do Fornecedor
        telefone = fake.phone_number()
        email_contato = fake.email()
        
        # 5% de chance de faltar email ou telefone (Simulando problemas de Frontend)
        if random.random() < 0.05:
            email_contato = "not_provided@domain.com"
        if random.random() < 0.05:
            telefone = "0000000000"
            
        termo_pagamento = random.choice(purchase_payment_certs.TERMOS_PAGAMENTO)
        certificado = random.choice(purchase_payment_certs.CERTIFICACOES_POSSIVEIS)
        
        tem_historico = random.choice([True, False, False, False, True])
        data_hoje = datetime.now() - timedelta(days=random.randint(30, 1000))

        if tem_historico:
            # --- LÓGICA SCD TIPO 2 (Declínio de Risco de Crédito) ---
            start_antigo = data_hoje - timedelta(days=random.randint(600, 1000))
            end_antigo = data_hoje - timedelta(days=random.randint(100, 500))
            
            # No passado, o fornecedor era bom (Rating Alto) e Ativo
            nota_antiga = random.randint(4, 5)

            payload_antigo = {
                "vendor_id": vendor_counter_id,
                "business_entity_id": vendor_counter_id, # Neste modelo, simplificamos 1:1
                "account_number": f"ACC-{vendor_counter_id:06d}",
                "vendor_name": nome_empresa,
                "credit_rating": nota_antiga,
                "vendor_details": {
                    "tax_id": cnpj,
                    "payment_terms": termo_pagamento,
                    "certifications": random.choices(purchase_payment_certs.CERTIFICACOES_POSSIVEIS, k=random.randint(1, 3)),
                    "primary_contact": {
                        "name": fake.name(),
                        "email": email_contato,
                        "phone": telefone
                    },
                    "average_lead_time_days": random.randint(5, 45),
                    "on_time_delivery_rate": round(random.uniform(0.7, 0.99), 2)
                },
                "purchasing_webservice_url": f"https://api.{fake.domain_name()}/orders",
                
                "effective_start_date": start_antigo,
                "effective_end_date": end_antigo,
                "is_current": False,

                "created_at": start_antigo,
                "updated_at": end_antigo,
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
            
            # Validação Strict via Pydantic
            vendor_antigo = Vendor(**payload_antigo).model_dump()
            batch.append(vendor_antigo)

            # ATUAL: O rating despencou para 1 ou 2. 
            # Como a Is_Active defaulta para True, o Validador Pydantic barraria!
            # Precisamos forçar a inativação na geração atual.
            start_atual = end_antigo + timedelta(days=random.randint(1, 30))
            nota_atual = random.randint(1, 2)
            ativo_atual = False # Forçada inativação por risco alto (Aprovado pelo validador Pydantic)

        else:
            # --- FORNECEDOR NOVO (Sem Histórico) ---
            start_atual = data_hoje - timedelta(days=random.randint(1, 30))
            nota_atual = random.randint(3, 5) # Fornecedores novos começam bons
            ativo_atual = True

        payload_atual = {
            "vendor_id": vendor_counter_id,
            "business_entity_id": vendor_counter_id,
            "account_number": f"ACC-{vendor_counter_id:06d}",
            "vendor_name": nome_empresa,
            "credit_rating": nota_atual,
            "vendor_details": {
                "tax_id": cnpj,
                "payment_terms": termo_pagamento,
                # Pode ter ganho certificações novas
                "certifications": random.choices(purchase_payment_certs.CERTIFICACOES_POSSIVEIS, k=random.randint(1, 3)),
                "primary_contact": {
                    "name": fake.name(),
                    "email": email_contato,
                    "phone": telefone
                },
                "average_lead_time_days": random.randint(5, 45),
                "on_time_delivery_rate": round(random.uniform(0.7, 0.99), 2)
            },
            "purchasing_webservice_url": f"https://api.{fake.domain_name()}/orders",
            
            "effective_start_date": start_atual,
            "effective_end_date": None,
            "is_current": True,

            "created_at": start_atual,
            "updated_at": start_atual,
            "is_active": ativo_atual,
            "row_guid": uuid.uuid4()
        }
        
        # Passa pela malha fina do Pydantic (Validação das regras de negócio)
        vendor_atual = Vendor(**payload_atual).model_dump()
        batch.append(vendor_atual)

        vendor_counter_id += 1

    return batch

if __name__ == "__main__":
    import json
    # Para o dump de teste, configuramos default=str por causa de uuid e datetime não serializáveis pelo json nativo
    fornecedores = get_vendor_details(5)
    print(f"Total de registros de fornecedor gerados (incluindo SCD2 histórico): {len(fornecedores)}")
    print("-" * 50)
    
    # Procura um Fornecedor que tenha Histórico (is_current == False)
    hist = [f for f in fornecedores if not f["is_current"]]
    if hist:
        historico = hist[0]
        # Pega o registro atual do mesmo fornecedor
        atual = next(f for f in fornecedores if f["vendor_id"] == historico["vendor_id"] and f["is_current"])
        
        print("--- DEMONSTRAÇÃO DE SCD TIPO 2 E VALIDAÇÃO PYDANTIC ---")
        print("\n[PASSADO] - Fornecedor Ativo e com Bom Rating de Crédito")
        print(f"Vendor: {historico['vendor_name']} | Rating: {historico['credit_rating']} | is_active: {historico['is_active']} | Data Inicial: {historico['effective_start_date']}")
        
        print("\n[PRESENTE] - Rating Despencou. Bloqueado por Risco de Crédito (Validação)")
        print(f"Vendor: {atual['vendor_name']} | Rating: {atual['credit_rating']} | is_active: {atual['is_active']} | Data Inicial: {atual['effective_start_date']}")
        print("-" * 50)
    
    print("\n--- AMOSTRA JSON DO PAYLOAD COMPLETO DO ÚLTIMO REGISTRO GERADO ---")
    print(json.dumps(fornecedores[-1], indent=2, default=str))
