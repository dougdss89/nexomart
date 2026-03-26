---
description: Como criar novos pipelines do zero no Data Platform mantendo toda a arquitetura base
---

# 🏗️ Blueprint: Criando Novas Áreas Analíticas (Schemas/Pipelines)

Quando o Engenheiro (USER) solicitar a criação de um novo Pipeline de Dados abstrato (ex: Vendas / `Sales`, Logística / `Logistics`), você DEVE seguir exatamente a hierarquia abaixo:

### Passo 1: O Modelo de Banco de Dados (`api/models/`)
1. Crie o model SQLAlchemy.
2. Estenda `Base` e `AuditMixin`.
3. Force `__tablename__` em minúsculo.
4. Índices JSONB devem ser declarados com casting e mapear as chaves `snake_case` nativas.

### Passo 2: O Schema Pydantic (`api/schemas/`)
1. Crie as propriedades espelhando `1:1` as chaves do Model (SQL).
2. Adicione restrições rigorosas em Enums estendidos por string.
3. Use UUID e Booleans padrão.
4. Implemente um `model_validator` temporizando datas ou travando lógicas de negócios corrompidas.

### Passo 3: O Seeder de Dados Realísticos (`api/seeder/`)
1. Se esse Pipeline herda dados de outro (ex: Venda herda de Cliente), faça o import do array gerado no script anterior (ex: `get_person_customer_seeders()`).
2. Cruze as informações com tabelas satélite (`samples/`).
3. Construa o gerador injetando o payload JSON bruto na classe Schema instanciada do Pydantic (`PipelineSchema(**payload).model_dump()`) para blindar o dado.
4. Se for fato numérico (Time-Series): Aplique Matemática de Funil.
5. Se for entidade (Dimensão): Aplique `Chaos Engineering` (5-10% erros sintáticos controlados).

### Passo 4: A Tríade da Arquitetura (Repository, Service, Router)
**É estritamente obrigatório** que ao concluir a geração do Seeder você proativamente crie a base de API REST 3-Layer para acompanhar o schema:
1. Crie o `_repository.py` em `api/repositories` para fazer o `bulk_insert()` assíncrono via SQLAlchemy puro.
2. Crie o `_service.py` abstrato com as precondições analíticas.
3. Exponha a rota Pydantic em `api/routers` vinculada ao `Depends(get_db)`.

### Passo 5: Integração CLI / Airflow (Test Output)
1. Antes de declarar "concluído", o script Seeder no escopo `__main__` DEVE expor parâmetros para o terminal via `argparse` (ex: `--count` ou `--batch_size`) para viabilizar o disparo autônomo via `BashOperator` do Airflow.
2. Certifique-se absolutamente de que nenhum *for-loop* esteja enviando as queries individuais para o banco (todas as operações devem passar pela Tríade como lotes).
3. Teste no terminal e avise o USER.
