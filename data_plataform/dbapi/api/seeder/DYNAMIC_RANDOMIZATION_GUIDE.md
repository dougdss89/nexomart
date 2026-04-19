# Guia de Randomização Dinâmica — Seeders Imprevisíveis

Este documento explica como usar os seeders com **randomização dinâmica** de volumes e chaos rates, tornando cada execução da DAG do Airflow única e imprevisível.

## 🎯 Objetivo

Criar um ambiente de dados realista onde:
- **Volumes variam** a cada execução (dias normais vs picos de demanda)
- **Chaos rates variam** a cada execução (1% a 10% de anomalias)
- **Tudo é configurável** via ranges min/max por schema
- **Totalmente integrado** com Airflow (BashOperator)

---

## 📦 Arquivos Criados

### 1. [api/config/seeder_config.py](../config/seeder_config.py)
**Configuração centralizada de ranges dinâmicos**

Define ranges min/max para cada schema/tabela:

```python
from api.config.seeder_config import get_ranges

# Obter ranges para sales
ranges = get_ranges(table="sales.salesorder")
print(ranges.min_count)  # 100_000
print(ranges.max_count)  # 50_000_000
print(ranges.min_chaos)  # 0.01
print(ranges.max_chaos)  # 0.10
```

**Ranges configurados:**

| Schema | Volume Min | Volume Max | Chaos Min | Chaos Max |
|--------|------------|------------|-----------|-----------|
| sales | 100k | 50kk | 1% | 10% |
| purchasing | 50k | 50kk | 1% | 10% |
| warehouse | 100k | 50kk | 2% | 10% |
| person | 10k | 5kk | 3% | 10% |
| product | 500 | 1.500 | 2% | 5% |
| humanresources | 50 | 100 | 0% | 0% |
| marketing | 1k | 500k | 1% | 8% |

### 2. [sales/sales_order_EXAMPLE_WITH_CHAOS.py](./sales/sales_order_EXAMPLE_WITH_CHAOS.py)
**Seeder de exemplo com randomização dinâmica**

Características:
- ✅ Importa ranges de `seeder_config.py`
- ✅ Aceita `--random` para randomizar valores
- ✅ Aceita `--min-count` / `--max-count` customizados
- ✅ Aceita `--min-chaos` / `--max-chaos` customizados
- ✅ Validação de limites absolutos (para product)
- ✅ Tipos de chaos recomendados automaticamente

**Uso:**

```bash
# 1. Modo randômico (recomendado para Airflow)
uv run python sales_order_EXAMPLE_WITH_CHAOS.py --random

# 2. Ranges customizados
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --min-count 100000 --max-count 1000000 \
    --min-chaos 0.01 --max-chaos 0.05 \
    --random

# 3. Valores fixos (desenvolvimento)
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --count 1000 --chaos-rate 0.05

# 4. Sem chaos
uv run python sales_order_EXAMPLE_WITH_CHAOS.py \
    --count 1000 --no-chaos
```

### 3. [airflow/dags/EXAMPLE_seeder_dag_with_randomization.py](../../airflow/dags/EXAMPLE_seeder_dag_with_randomization.py)
**DAG do Airflow com randomização**

Características:
- ✅ Executa todos os seeders com `--random`
- ✅ Respeita dependências (dimensões → fatos → analytics)
- ✅ Execução paralela quando possível
- ✅ Logs detalhados de valores sorteados
- ✅ Retries e alertas configurados

**Schedule:** Diariamente às 02:00

---

## 🚀 Como Funciona

### Fluxo de Randomização

```
┌─────────────────────────────────────┐
│  DAG do Airflow (diária às 02:00)  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  BashOperator chama seeder com:    │
│  --random --min-count X --max-count Y│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Seeder sorteia valores:            │
│  count = random.randint(min, max)   │
│  chaos = random.uniform(min, max)   │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Gera dados limpos (count registros)│
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Aplica chaos engineering (chaos %) │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  Insere em lote no banco (bulk)     │
└─────────────────────────────────────┘
```

### Exemplo de Execução

**Execução 1 (dia normal):**
```
🎲 MODO RANDÔMICO ATIVADO
   📊 Range de volume: 100,000 a 50,000,000
   📊 Range de chaos: 1.00% a 10.00%

   🎯 Valores sorteados:
      - Volume: 150,000 registros
      - Chaos rate: 3.45%

📣 Semeando 150,000 pedidos de venda
   - Batch size: 10,000
   - Chaos rate: 3.45%
   - Registros existentes: 0

   ⏳ Progresso: 10,000/150,000 (6.7%)
   ⏳ Progresso: 20,000/150,000 (13.3%)
   ...
   ⏳ Progresso: 150,000/150,000 (100.0%)

✅ Seeder finalizado!
   - Registros inseridos: 150,000
   - Total na tabela: 150,000
   - Anomalias injetadas: ~5,175 (3.45%)
```

**Execução 2 (Black Friday simulada):**
```
🎲 MODO RANDÔMICO ATIVADO
   🎯 Valores sorteados:
      - Volume: 35,000,000 registros
      - Chaos rate: 8.92%

✅ Seeder finalizado!
   - Registros inseridos: 35,000,000
   - Total na tabela: 35,150,000
   - Anomalias injetadas: ~3,122,000 (8.92%)
```

---

## 🔧 Migração de Seeders Existentes

### Passo 1: Importar Configurações

No início do seeder:

```python
from api.config.seeder_config import get_ranges, get_recommended_chaos_types

# Obter ranges configurados
SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"
RANGES = get_ranges(table=TABLE_NAME)
```

### Passo 2: Adicionar Argumentos CLI

```python
parser.add_argument(
    "--random",
    action="store_true",
    help="Sorteia valores dentro dos ranges configurados"
)
parser.add_argument(
    "--min-count",
    type=int,
    default=RANGES.min_count,
    help=f"Volume mínimo (padrão: {RANGES.min_count:,})"
)
parser.add_argument(
    "--max-count",
    type=int,
    default=RANGES.max_count,
    help=f"Volume máximo (padrão: {RANGES.max_count:,})"
)
parser.add_argument(
    "--min-chaos",
    type=float,
    default=RANGES.min_chaos,
    help=f"Chaos rate mínimo (padrão: {RANGES.min_chaos:.2%})"
)
parser.add_argument(
    "--max-chaos",
    type=float,
    default=RANGES.max_chaos,
    help=f"Chaos rate máximo (padrão: {RANGES.max_chaos:.2%})"
)
```

### Passo 3: Implementar Lógica de Randomização

```python
if args.random:
    # MODO RANDÔMICO: sortear valores
    count = random.randint(args.min_count, args.max_count)
    chaos_rate = random.uniform(args.min_chaos, args.max_chaos) if not args.no_chaos else 0.0

    print("🎲 MODO RANDÔMICO ATIVADO")
    print(f"   🎯 Valores sorteados:")
    print(f"      - Volume: {count:,} registros")
    print(f"      - Chaos rate: {chaos_rate:.2%}")
else:
    # MODO FIXO: usar valores especificados
    count = args.count
    chaos_rate = args.chaos_rate
```

### Passo 4: Usar Tipos de Chaos Recomendados

```python
# Obter tipos de chaos recomendados automaticamente
chaos_types = get_recommended_chaos_types(SCHEMA_NAME)

# Aplicar chaos
if apply_chaos and chaos_types:
    orders_with_chaos = apply_data_quality_chaos(
        orders,
        chaos_rate=chaos_rate,
        allowed_chaos_types=chaos_types,
        verbose=False
    )
```

---

## 📋 Checklist de Migração

Para cada seeder:

- [ ] Importar `get_ranges` e `get_recommended_chaos_types`
- [ ] Definir `SCHEMA_NAME` e `TABLE_NAME`
- [ ] Obter `RANGES = get_ranges(table=TABLE_NAME)`
- [ ] Adicionar argumentos CLI (`--random`, `--min-count`, etc.)
- [ ] Implementar lógica de randomização (`if args.random:`)
- [ ] Usar `get_recommended_chaos_types(SCHEMA_NAME)`
- [ ] Testar com `--random`
- [ ] Testar com valores fixos (`--count`)
- [ ] Adicionar à DAG do Airflow

---

## 🎲 Exemplo Completo de Migração

### Antes (seeder antigo):
```python
parser.add_argument("--count", type=int, default=1000)

count = args.count
orders = generate_sales_order_payloads(count, ...)
stmt = insert(SalesOrderModel).values(orders)
```

### Depois (seeder com randomização):
```python
from api.config.seeder_config import get_ranges, get_recommended_chaos_types

SCHEMA_NAME = "sales"
TABLE_NAME = "sales.salesorder"
RANGES = get_ranges(table=TABLE_NAME)

parser.add_argument("--random", action="store_true")
parser.add_argument("--min-count", type=int, default=RANGES.min_count)
parser.add_argument("--max-count", type=int, default=RANGES.max_count)
parser.add_argument("--min-chaos", type=float, default=RANGES.min_chaos)
parser.add_argument("--max-chaos", type=float, default=RANGES.max_chaos)

if args.random:
    count = random.randint(args.min_count, args.max_count)
    chaos_rate = random.uniform(args.min_chaos, args.max_chaos)
    print(f"🎲 Sorteado: {count:,} registros | Chaos: {chaos_rate:.2%}")
else:
    count = args.count
    chaos_rate = args.chaos_rate

orders = generate_sales_order_payloads(count, ...)
chaos_types = get_recommended_chaos_types(SCHEMA_NAME)
orders_with_chaos = apply_data_quality_chaos(orders, chaos_rate, chaos_types)
stmt = insert(SalesOrderModel).values(orders_with_chaos)
```

---

## 🐛 Troubleshooting

### Problema 1: Valores não estão randomizando
**Causa**: Esqueceu de passar `--random` na CLI ou DAG

**Solução**:
```bash
# ❌ Errado (usa valores fixos)
python seeder.py --count 1000

# ✅ Correto (randomiza)
python seeder.py --random
```

### Problema 2: Chaos rate sempre zero
**Causa**: `--no-chaos` está ativo ou range min/max é 0.0

**Solução**:
```bash
# Verificar ranges
python -c "from api.config.seeder_config import get_ranges; print(get_ranges('sales'))"

# Forçar chaos
python seeder.py --random --min-chaos 0.05 --max-chaos 0.10
```

### Problema 3: Volume ultrapassou limite absoluto (product)
**Causa**: Não está validando `absolute_max`

**Solução**:
```python
if RANGES.absolute_max:
    existing = await session.scalar(select(func.count(ProductModel.product_id)))
    count = min(count, RANGES.absolute_max - existing)
```

### Problema 4: DAG do Airflow não randomiza
**Causa**: Comando no BashOperator sem `--random`

**Solução**:
```python
# ❌ Errado
bash_command="python seeder.py --count 1000"

# ✅ Correto
bash_command=f"python seeder.py --random --min-count {min} --max-count {max}"
```

---

## 📊 Monitoramento e Auditoria

### Logs Importantes

Cada seeder deve gerar logs com:

```
🎲 MODO RANDÔMICO ATIVADO
   📊 Range de volume: 100,000 a 50,000,000
   📊 Range de chaos: 1.00% a 10.00%

   🎯 Valores sorteados:
      - Volume: 1,234,567 registros
      - Chaos rate: 5.67%

📣 Semeando 1,234,567 pedidos de venda (Fact Table)
   - Batch size: 10,000
   - Chaos rate: 5.67%
   - Registros existentes: 0

   ⏳ Progresso: 10,000/1,234,567 (0.8%)
   ...

✅ Seeder finalizado!
   - Registros inseridos: 1,234,567
   - Total na tabela: 1,234,567
   - Anomalias injetadas: ~70,000 (5.67%)
```

### Queries de Auditoria

```sql
-- Total de registros por tabela
SELECT
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname IN ('sales', 'purchasing', 'warehouse', 'person')
ORDER BY n_live_tup DESC;

-- Distribuição de anomalias (exemplo: CPFs inválidos)
SELECT
    'Invalid CPF' as anomaly_type,
    COUNT(*) as count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person.customer) as percentage
FROM person.customer
WHERE cpf IN ('000.000.000-00', '111.111.111-11', '123.456.789-00');
```

---

## 🔄 Customização de Ranges

### Por Schema

Editar [`api/config/seeder_config.py`](../config/seeder_config.py):

```python
SCHEMA_RANGES = {
    "sales": SchemaRanges(
        min_count=100_000,      # ← Ajustar aqui
        max_count=50_000_000,   # ← Ajustar aqui
        min_chaos=0.01,         # ← Ajustar aqui
        max_chaos=0.10,         # ← Ajustar aqui
        description="..."
    ),
}
```

### Por Tabela Específica

```python
TABLE_RANGES = {
    "sales.salesorder": SchemaRanges(
        min_count=200_000,      # Override para esta tabela
        max_count=60_000_000,
        min_chaos=0.02,
        max_chaos=0.12,
        description="..."
    ),
}
```

### Via CLI (Override Temporário)

```bash
# Forçar ranges específicos para uma execução
python seeder.py --random \
    --min-count 500000 \
    --max-count 1000000 \
    --min-chaos 0.05 \
    --max-chaos 0.08
```

---

## 📚 Referências

- **Configuração de Ranges**: [api/config/seeder_config.py](../config/seeder_config.py)
- **Exemplo de Seeder**: [sales/sales_order_EXAMPLE_WITH_CHAOS.py](./sales/sales_order_EXAMPLE_WITH_CHAOS.py)
- **Exemplo de DAG**: [airflow/dags/EXAMPLE_seeder_dag_with_randomization.py](../../airflow/dags/EXAMPLE_seeder_dag_with_randomization.py)
- **Chaos Engineering**: [CHAOS_DOCUMENTATION.md](./CHAOS_DOCUMENTATION.md)
- **Migração Geral**: [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md)
- **Regras Gerais**: [CLAUDE.md](../../CLAUDE.md)

---

## ✅ Conclusão

Com randomização dinâmica, cada execução da DAG do Airflow é **única e imprevisível**, simulando cenários realistas de produção:

- 📊 **Dias normais**: 100k registros, 1% chaos
- 🔥 **Picos de demanda**: 50kk registros, 10% chaos
- 🎲 **Totalmente randômico**: valores diferentes a cada execução

Isso permite:
- Testar pipelines de limpeza com volumes variados
- Treinar modelos de detecção de fraudes com diferentes taxas de anomalias
- Validar escalabilidade do sistema (dias normais vs picos)
- Simular ambientes realistas de produção
