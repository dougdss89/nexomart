import uuid
import random
from datetime import datetime
from typing import List, Dict, Any
from faker import Faker
from api.seeder.chaos import apply_customer_email_chaos

fake = Faker('pt_BR')

def generate_mock_emails(customer: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Gera as linhas de e-mail para um único cliente.
    Pode gerar 1 ou mais e-mails.
    Injeta anomalias (erros de digitação, falta de @, domínios sujos).
    """
    emails = []
    
    # Decobre o domínio baseado no tipo de cliente
    is_company = customer.get("customer_type") == "VC"
    base_name = str(customer.get("customer_name")).lower().replace(" ", "").replace(".", "").replace(",", "")
    
    # 1. Gerar o E-mail Principal
    primary_email = ""
    if is_company:
        domain = f"{base_name}.com.br"
        primary_email = f"contato@{domain}"
    else:
        domain = random.choice(["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br"])
        primary_email = f"{base_name}.{random.randint(1950, 2010)}@{domain}"
        
    # --- ANOMALIAS NO PRIMARY EMAIL ---
    if random.random() < 0.03:
        # Erro de formatação: Esquecer o '@'
        primary_email = primary_email.replace("@", "")
    elif random.random() < 0.02:
        # Erro de formatação: Espaço no e-mail
        primary_email = primary_email.replace("@", " @")
        
    emails.append({
        "customer_id": customer.get("customer_id", 0), # Será injetado pelo orquestrador
        "customer_email": primary_email,
        "is_primary": True,
        "email_uuid": str(uuid.uuid4()),
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
        "is_active": True,
        "row_guid": str(uuid.uuid4())
    })
    
    # 2. Gerar E-mails Secundários (20% de chance)
    if random.random() < 0.20:
        sec_email = f"financeiro@{domain}" if is_company else f"spam_{base_name}@{domain}"
        
        emails.append({
            "customer_id": customer.get("customer_id", 0),
            "customer_email": sec_email,
            "is_primary": False,
            "email_uuid": str(uuid.uuid4()),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "is_active": True,
            "row_guid": str(uuid.uuid4())
        })
        
    return emails

def get_customer_email_seeders(customers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Recebe a lista de clientes gerada anteriormente e cria os e-mails
    vinculando as Chaves Estrangeiras dinamicamente.
    """
    try:
        batch = []
        
        # Como o payload vindo do get_customer_seeders() inicialmente não tem 'customer_id' 
        # (pois o PostgreSQL é quem gera o autoincrement), no mundo do MOCK nós iremos
        # iterar os clientes usando um ID virtual correspondente ao índice.
        
        for idx, customer in enumerate(customers, start=1):
            
            # Adicionamos o ID virtual (Mock constraint)
            customer_with_id = customer.copy()
            customer_with_id["customer_id"] = idx
            
            # Chama a fábrica de e-mails do cliente atual
            emails_do_cliente = generate_mock_emails(customer_with_id)
            
            batch.extend(emails_do_cliente)

        return apply_customer_email_chaos(batch)

    except Exception as e:
        print(f"Erro ao gerar seeders de email: {e}")
        return []

# --- TESTANDO NO TERMINAL ---
if __name__ == "__main__":
    from api.seeder.person.person_customer import get_customer_seeders
    
    print("Gerando clientes para base do teste...")
    clientes_mock = get_customer_seeders(quantidade=3)
    
    print("Gerando e-mails mockados...")
    emails_mock = get_customer_email_seeders(clientes_mock)
    
    print(f"Total de clientes base: {len(clientes_mock)}")
    print(f"Total de e-mails gerados: {len(emails_mock)}")
    print("-" * 50)
    
    import json
    print(json.dumps(emails_mock, indent=2, default=str))
