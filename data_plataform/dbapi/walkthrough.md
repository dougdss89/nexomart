# Product Services Implementation Walkthrough

## 1. Summary of Actions
Para possibilitar a criação de dados para os schemas e models já definidos, implementei a camada de Serviços (`services`) com foco no schema `product`. Estes serviços têm o papel de receber dados (validados pelos schemas Pydantic) e realizar a persistência no banco via `SQLAlchemy AsyncSession`.

Durante o processo de planejamento, encontrei e resolvi os seguintes problemas nos modelos existentes para evitar conflitos de importação:
- **`ProductModel` vs `Product`**: Renomeei a classe no arquivo `product.py` para `Product` e a classe em `productcategory.py` de `ProductCategorModel` para `ProductCategoryModel` (fixando também o erro de digitação).

## 2. Changes Made
Foram criados os seguintes arquivos para compor as funções de backend da `dbapi`:

### Correção de Nomenclatura
- [api/models/product/product.py](file:///home/doug/project/data_plataform/dbapi/api/models/product/product.py) (`ProductModel` -> `Product`)
- [api/models/product/productcategory.py](file:///home/doug/project/data_plataform/dbapi/api/models/product/productcategory.py) (`ProductCategorModel` -> `ProductCategoryModel`)

### Novos Serviços (Padrão Batch/Bulk Insert para CDC)
- **[api/services/product/product_category_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_category_service.py)**: Serviço `bulk_insert_product_categories`
- **[api/services/product/product_model_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_model_service.py)**: Serviço `bulk_insert_product_models`
- **[api/services/product/product_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_service.py)**: Serviço `bulk_insert_products`
- **[api/services/product/product_embedding_service.py](file:///home/doug/project/data_plataform/dbapi/api/services/product/product_embedding_service.py)**: Serviço `bulk_insert_product_embeddings`

## 3. Dynamic Mock Data Generation (200k SKUs)
Para alcançar o objetivo de popular o banco de dados com cerca de 200.000 produtos realistas e diversificados:
- **[api/samples/product.py](file:///home/doug/project/data_plataform/dbapi/api/samples/product.py)**: Refatorado para ser um gerador combinatório avançado. Criei subclasses por categoria (como `SmartphoneSamples`, `LaptopSamples`, `WomensApparelSamples`, e novos como `MusicalInstrumentsSamples`, `GamingConsoleSamples`, etc.) munidas com listas de nomes-base, marcas, cores, tamanhos e variações de preço realista. A função `generate_randomized_catalog(n)` varre todas as fábricas dinamicamente e monta os produtos pseudo-reais que podem ultrapassar bilhões de iterações exclusivas.
- **[api/seeder/product/product.py](file:///home/doug/project/data_plataform/dbapi/api/seeder/product/product.py)**: Atualizado para não usar mais os blocos locais de roupas (hardcoded), mas sim consumir diretamente o `generate_randomized_catalog`. Ele cruza a saída com o seu padrão Enum (mapeando a FK segura) e injeta tudo em um layout amigável para o JSONB do banco de dados (respeitando UUIDs, data de inserção e etc).

## 4. Desafios de Data Engineering (SCD/CDC/Qualidade de Dados)
Como o objetivo do projeto é simular um ambiente real e complexo para ser tratado por pipelines de dados (ETL/ELT), adicionei intencionalmente características de "Dados Sujos" (Data Anomalies) na geração de produtos:
- **Schema Evolution Flexível**: Abri a `class Config(extra="allow")` no Pydantic do detalhe (`ProductDetails`). A rotina injeta propriedades JSONB que nem sempre existem em todos os produtos (como `is_refurbished`, `voltage`, ou `warranty_months`). Isso força o Data Engineer a tratar colunas dinâmicas (structs imprevisíveis).
- **Valores e Nulos**: Inseri ruídos de `nulls` (ex: ~4% de chance da profundidade `depth` não ser mapeada nas dimensões) e ~2% de chance do peso (`weight`) vir acompanhado por um `minus` (negativo, simulando erro de input humano).
- **Erros de Formatação (Typos)**: Criei uma probabilidade (~5%) de que as strings de `brand` fiquem em `UPPERCASE` e de `color` em `lowercase`, para exercitar rotinas de padronização e higienização durante o CDC ou consumo do Pub/Sub.
- **Campos de Imposto Zerados**: Taxas podem vir arbitrariamente como `0.0`.

## 5. Mock de Vetores de IA (Product Embedding Seeder)
Pensando na evolução do projeto para buscas semânticas, criei o **[api/seeder/product/product_embedding.py](file:///home/doug/project/data_plataform/dbapi/api/seeder/product/product_embedding.py)**:
- Ele gera vetores compatíveis com `pgvector`, simulando as saídas de grandes modelos (Ex: OpenAI Text-Embedding com 1536 dimensões e OpenAI CLIP-ViT para imagens com 512 dimensões).
- Injeta o log de metadados das simulações de IAs, incluindo falhas (Ex: quando a extração da imagem falha e retorna o array como `null`, gerando novos desafios de limpeza de dados).
- Gera o Hash SHA256 do texto original extraído das combinações do gerador. Esse hash permite a simulação futura de pipelines de controle de cache (para só reprocessar embeddings se a string textual original enviada via CDC for diferente do hash guardado no Postgres).

## 6. Engineering Best Practices (Services Layer)
Refatorei todos os quatro arquivos de `services` para prepararem o terreno para orquestração escalável no Airflow, aplicando os preceitos de **Software/Data Engineering**:
1. **Bulk Insert (SQLAlchemy 2.0 Core)**: Substituí a inserção via instâncias do ORM (`db.add`) pelo SQLAlchemy Core (`insert(Model).values()`). Quando você vai inserir **200.000 SKUs**, o overhead de instanciar o objeto ORM em memória estrangula o node. Com o `insert().values(lista_de_dicts)`, ele faz um bulk insert massivo e atômico, reduzindo a ingestão de horas para poucos minutos ou segundos.
2. **Robustez Transacional (Try/Except/Rollback)**: Adicionei o bloco `try/except SQLAlchemyError` que tenta fazer o commit. Se no meio dos 200k SKUs algum violar Foreign Key, ele pega a exceção, dá `db.rollback()` imediatamente e levanta um Log Erro. E o principal: o código faz um `raise` re-lançando a exceção, informando a ferramenta externa (O Airflow Sensor/Operator) que aquela Task quebrou, ativando automaticamente a fila de *retries* da DAG de forma limpa, ao invés da DAG achar que deu sucesso mas corromper o banco.
3. **Type Hinting e Early Returns (Guard Clauses)**: Tipagem na assinatura (`List[Dict[str, Any]]`) foi aplicada para linting com o Mypy e facilidade do desenvolvedor sênior bater o olho. Além disso, incluí `if not data: return False`, que evita abrir transações no Postgres inutilmente caso a listagem de insert chegue vazia no worker do Airflow.

Estes serviços podem ser importados convenientemente de `api.services.product` graças ao `__init__.py` configurado.

## 6. How to verify (Test)
Para validar o funcionamento integrado do seu banco de dados, SQLAlchemy e Pydantic sem precisar iniciar a API REST, você pode executar o script de teste que deixei em `/home/doug/project/data_plataform/dbapi/test_product_services.py`.

1. Acesse o seu container Docker (ou ambiente com acesso à URL do seu Postgres e com as dependências instaladas):
   ```bash
   cd /home/doug/project/data_plataform/dbapi
   uv run python test_product_services.py
   ```
2. O script fará a comunicação assíncrona, conectando-se e verificando passo a passo a persistência virtual de Categoria, Modelo, Produto base, e seus embeddings. (Ele utiliza um `rollback` ao final para não sujar sua base com mock de teste).

> Mantenha em mente que isso prepara o terreno perfeitamente para implementarmos as rotas FastAPI no futuro.
