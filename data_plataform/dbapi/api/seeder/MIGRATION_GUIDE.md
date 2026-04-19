# Guia de Migração — Seeders com Chaos Engineering

Este documento explica como migrar os seeders existentes para o novo padrão com chaos engineering e limites de volume.

## Resumo das Mudanças

### 1. Novos Arquivos Criados

| Arquivo | Descrição |
|---------|-----------|
| [`CHAOS_DOCUMENTATION.md`](./CHAOS_DOCUMENTATION.md) | Documentação completa de todas as anomalias injetadas |
| [`api/utils/chaos_engineering.py`](../utils/chaos_engineering.py) | Módulo utilitário com funções de chaos engineering |
| [`sales/sales_order_EXAMPLE_WITH_CHAOS.py`](./sales/sales_order_EXAMPLE_WITH_CHAOS.py) | Exemplo de seeder com novas regras aplicadas |

### 2. Atualizações no CLAUDE.md

O arquivo [`CLAUDE.md`](../../CLAUDE.md) foi atualizado com:
- **Limites de volume por schema** (seção "Data Volume Limits per Schema")
- **Regras de qualidade de dados** (seção "Data Quality Rules — Chaos Engineering")
- **Implementação de chaos engineering** nos seeders

## Mudanças Necessárias nos Seeders Existentes

### Passo 1: Adicionar Validação de Limites

Para seeders dos schemas `product`, adicionar validação **antes** de inserir:

```python
from sqlalchemy import func

# Exemplo: seeder de products
MAX_PRODUCTS = 100_000

existing_count = await session.scalar(select(func.count(ProductModel.product_id)))
if existing_count + count > MAX_PRODUCTS:
    print(f"⚠️ Limite de produtos atingido ({existing_count}/{MAX_PRODUCTS}).")
    print(f"   Só é possível inserir mais {MAX_PRODUCTS - existing_count} registros.")
    return
```

### Passo 2: Importar Módulo de Chaos Engineering

Adicionar no início do arquivo:

```python
from api.utils.chaos_engineering import apply_data_quality_chaos, generate_chaos_metadata
```

### Passo 3: Aplicar Chaos Engineering Antes da Inserção

**ANTES** (código antigo):
```python
orders = generate_sales_order_payloads(count, customer_ids, ...)

stmt = insert(SalesOrderModel).values(orders)
await session.execute(stmt)
```

**DEPOIS** (novo código):
```python
# 1. Gerar dados limpos
orders = generate_sales_order_payloads(count, customer_ids, ...)

# 2. Aplicar chaos engineering (5% de anomalias)
orders_with_chaos = apply_data_quality_chaos(
    orders,
    chaos_rate=0.05,
    allowed_chaos_types=[
        "duplicate", "fraud_fake_cpf", "incomplete_payment"
    ],
    verbose=True
)

# 3. Inserir com anomalias
stmt = insert(SalesOrderModel).values(orders_with_chaos)
await session.execute(stmt)
```

### Passo 4: Adicionar Argumentos CLI

Atualizar `argparse` para permitir controle de chaos rate:

```python
parser.add_argument("--chaos-rate", type=float, default=0.05, help="Percentual de anomalias (0.01 a 0.10)")
parser.add_argument("--no-chaos", action="store_true", help="Desabilita chaos engineering")
```

### Passo 5: Gerar Metadados (Opcional)

Para documentação, gerar metadados de chaos:

```python
if apply_chaos:
    metadata = generate_chaos_metadata(
        table_name="sales.salesorder",
        chaos_rate=chaos_rate,
        chaos_types=["duplicate", "fraud_fake_cpf", ...]
    )
    print(f"\n📊 Chaos Metadata: {metadata}")
```

## Tipos de Chaos por Schema

### Sales

Tipos de chaos recomendados:
- `duplicate` — Registros duplicados
- `dirty_cpf` — CPF inválido
- `dirty_email` — Email malformado
- `fraud_fake_cpf` — CPF falso com nome real
- `fraud_cloned_card` — Compras geograficamente impossíveis
- `fraud_high_value_suspicious` — Alto valor com dados suspeitos
- `incomplete_payment` — Pagamento pendente > 30 dias
- `incomplete_shipping` — Pago mas não enviado > 15 dias
- `incomplete_delivery` — Enviado mas não entregue > 90 dias

### Purchasing

Tipos de chaos recomendados:
- `duplicate` — Registros duplicados
- `fraud_duplicate_order` — Ordens duplicadas para mesmo fornecedor
- `fraud_phantom_vendor` — Fornecedores fantasmas
- `fraud_overpriced` — Superfaturamento (3-8x acima)
- `negative_cost` — Custos negativos
- `missing_vendor_ref` — Compra sem fornecedor

### Warehouse

Tipos de chaos recomendados:
- `warehouse_lost_transfer` — Transferências perdidas (> 90 dias)
- `warehouse_quantity_mismatch` — Divergência de quantidade
- `warehouse_negative_stock` — Estoque negativo
- `warehouse_missing_location` — Produto sem localização física
- `warehouse_quality_fail` — Produto reprovado mas enviado
- `warehouse_cold_chain_break` — Cold chain quebrada
- `warehouse_same_location_transfer` — Transferência para mesmo local
- `warehouse_missing_traceability` — Sem rastreabilidade
- `missing_order_ref` — Movimentação sem ordem de origem

### Person

Tipos de chaos recomendados:
- `duplicate` — Registros duplicados
- `dirty_cpf` — CPF inválido
- `dirty_phone` — Telefone inválido
- `dirty_email` — Email malformado
- `dirty_cep` — CEP inválido
- `dirty_name` — Nome inválido
- `fraud_fake_address` — Nome falso com endereço real

### HumanResources

**Não aplicar chaos engineering** — Volume muito baixo (100 linhas) e dados críticos para RH.

### Product

**Aplicar chaos moderado** (2-3%) — Volume controlado (máx 100k):
- `duplicate` — Produtos duplicados
- `outlier_value` — Preços muito altos/baixos

## Exemplo Completo de Migração

Veja o arquivo [`sales/sales_order_EXAMPLE_WITH_CHAOS.py`](./sales/sales_order_EXAMPLE_WITH_CHAOS.py) para um exemplo completo de seeder migrado.

## Testes Recomendados

Após migrar um seeder, execute testes para validar:

1. **Teste com dados limpos** (sem chaos):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --no-chaos
   ```

2. **Teste com chaos baixo** (1%):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --chaos-rate 0.01
   ```

3. **Teste com chaos alto** (10%):
   ```bash
   uv run python api/seeder/sales/sales_order.py --count 100 --chaos-rate 0.10
   ```

4. **Verificar anomalias injetadas**:
   ```sql
   -- CPFs inválidos
   SELECT COUNT(*) FROM person.customer WHERE cpf IN ('000.000.000-00', '111.111.111-11');

   -- Duplicatas (case-insensitive)
   SELECT UPPER(customer_name), COUNT(*) FROM person.customer GROUP BY UPPER(customer_name) HAVING COUNT(*) > 1;

   -- Estoque negativo
   SELECT COUNT(*) FROM warehouse.product_inventory WHERE quantity_on_hand < 0;
   ```

## Priorização de Migração

### Alta Prioridade (Schemas Críticos)

1. **`sales/sales_order.py`** — Fato principal de vendas
2. **`purchasing/purchase_purchaseorderheader.py`** — Fato principal de compras
3. **`warehouse/transaction_history.py`** — Fato principal de movimentações
4. **`person/person_customer.py`** — Dimensão crítica de clientes

### Média Prioridade

5. **`sales/sales_return_order.py`** — Devoluções (importante para análise de qualidade)
6. **`warehouse/product_inventory.py`** — Estoque atual
7. **`purchasing/purchase_purchaseorderdetail.py`** — Detalhes de compras

### Baixa Prioridade

8. Seeders de dimensões estáveis (marketing, product categories, etc.)
9. Seeders de lookup tables (channels, transaction types, etc.)

## Checklist de Migração

Para cada seeder migrado:

- [ ] Adicionar validação de limites (se aplicável)
- [ ] Importar `apply_data_quality_chaos` e `generate_chaos_metadata`
- [ ] Aplicar chaos engineering antes da inserção
- [ ] Adicionar argumentos CLI (`--chaos-rate`, `--no-chaos`)
- [ ] Escolher tipos de chaos apropriados para o domínio
- [ ] Testar com dados limpos e com chaos
- [ ] Verificar anomalias injetadas no banco
- [ ] Atualizar documentação (comentários no código)
- [ ] Gerar metadados de chaos (opcional)

## Próximos Passos

1. **Migrar seeders prioritários** (sales, purchasing, warehouse, person)
2. **Criar dashboards de data quality** (Trino + queries analíticas)
3. **Implementar pipelines de limpeza** (dbt models)
4. **Treinar modelos de detecção de fraudes** (ML)
5. **Integrar com ferramentas de validação** (Great Expectations, Soda)

## Dúvidas Frequentes

### 1. Por que injetar dados ruins intencionalmente?

Para simular cenários reais de engenharia de dados e testar pipelines de limpeza, detecção de fraudes e governança.

### 2. Posso desabilitar chaos engineering?

Sim, use `--no-chaos` para gerar apenas dados limpos (útil para testes ou ambientes de produção sem necessidade de chaos).

### 3. Como escolher o chaos_rate ideal?

- **1-3%**: Ambientes de produção ou validação final
- **5%** (padrão): Desenvolvimento e testes de pipelines
- **10%**: Testes de stress de sistemas de limpeza

### 4. Como documentar as anomalias injetadas?

Use `generate_chaos_metadata()` e salve em arquivo JSON ou banco de dados de metadados.

### 5. Como validar que o chaos foi aplicado corretamente?

Execute queries de detecção (veja seção "Testes Recomendados") e verifique se as contagens estão próximas do chaos_rate esperado.

---

**Documentação**: Para mais detalhes sobre tipos de anomalias, veja [`CHAOS_DOCUMENTATION.md`](./CHAOS_DOCUMENTATION.md).
