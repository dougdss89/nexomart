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

### Passo 4: Verificação Terminal (Test Output)
1. Antes de declarar "concluído", chame a função principal em um loop `__main__` e dispare via `uv run python api/seeder/suapipeline.py`.
2. Trate exceções e avise o USER.
