# Chaos Engineering Documentation — Data Quality Anomalies

Este documento descreve todas as anomalias intencionalmente injetadas nos dados pelos seeders para simular cenários reais de engenharia de dados.

## Objetivo

Criar um ambiente de testes realista que permite:
1. Desenvolver e testar pipelines de limpeza de dados
2. Treinar modelos de detecção de fraudes
3. Validar regras de qualidade de dados
4. Preparar analistas para cenários reais de produção

## Taxa de Anomalias

Todos os seeders dos schemas `sales`, `purchasing`, `warehouse`, `person` injetam **entre 1% e 10%** de anomalias, distribuídas conforme especificado abaixo.

---

## 1. Anomalias de Qualidade de Dados

### 1.1 Duplicidade (1-5%)

**Descrição**: Registros duplicados com pequenas variações que dificultam a detecção automática.

**Tabelas Afetadas**: `person.customer`, `sales.salesorder`, `purchasing.purchaseorderheader`

**Tipos de Duplicação**:

| Tipo | Exemplo Original | Exemplo Duplicado | Técnica |
|------|------------------|-------------------|---------|
| Case sensitivity | `"João Silva"` | `"JOÃO SILVA"` | Upper/lowercase |
| Espaçamento | `"Maria Santos"` | `"Maria  Santos "` | Espaços extras, leading/trailing |
| Formatação de CPF | `"123.456.789-00"` | `"12345678900"` | Com/sem formatação |
| Caracteres especiais | `"José"` | `"Jose"` | Acentuação removida |
| Ordem de nomes | `"Silva, João"` | `"João Silva"` | Inversão de ordem |

**Cenários de Teste**:
- Fuzzy matching com Levenshtein distance
- Normalização de strings (unaccent, trim, lowercase)
- Detecção de duplicatas via embeddings
- Merge de registros duplicados com escolha de golden record

---

### 1.2 Dados Sujos (2-10%)

**Descrição**: Campos com valores malformados que precisam de regex, normalização ou enriquecimento.

#### 1.2.1 Telefones Inválidos

**Tabelas**: `person.customer`, `person.customer_email`, `purchasing.vendor`

| Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-----------------|--------------|---------------------|
| `"0000000"` | Placeholder | Marcar como inválido, solicitar redigitação |
| `"11111111111"` | Sequência | Validar dígitos únicos |
| `"(00) 0000-0000"` | Formato sem número real | Validar contra lista de códigos de área válidos |
| `"99 99999-9999"` | DDD inexistente | Validar DDD contra tabela ANATEL |
| `"+55 11 91234-567"` | Dígitos faltando | Validar comprimento (fixo: 10, celular: 11) |

#### 1.2.2 CEPs Inválidos

**Tabelas**: `person.address`, `warehouse.warehouselocation`

| Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-----------------|--------------|---------------------|
| `"00000-000"` | Placeholder | Enriquecer via API ViaCEP ou similar |
| `"XXXXX-XXX"` | Não numérico | Validar formato e existência |
| `"01310-100"` (SP) + `state: "RJ"` | Inconsistência geográfica | Cruzar CEP com UF esperada |
| `"12345-678"` | CEP inexistente | Validar contra base dos Correios |

#### 1.2.3 CPF/CNPJ Inválidos

**Tabelas**: `person.customer`, `purchasing.vendor`, `humanresources.employee`

| Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-----------------|--------------|---------------------|
| `"000.000.000-00"` | Placeholder | Marcar como inválido |
| `"111.111.111-11"` | Sequência | Bloquear sequências repetidas |
| `"123.456.789-09"` | Dígito verificador errado | Recalcular e corrigir ou invalidar |
| `"12345678901234"` (14 dígitos sem formatação) | Confusão CPF/CNPJ | Detectar tamanho e formatar corretamente |
| CPF válido matematicamente mas não cadastrado na Receita | CPF sintético | Enriquecer com base da Receita (se disponível) |

#### 1.2.4 Emails Inválidos

**Tabelas**: `person.customer_email`, `humanresources.employee`

| Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-----------------|--------------|---------------------|
| `"email@dominio"` | TLD faltando | Validar formato RFC 5322 |
| `"@dominio.com"` | Usuário faltando | Rejeitar ou solicitar correção |
| `"email@@dominio.com"` | Caracteres duplicados | Normalizar antes de validar |
| `"email@dominio .com"` | Espaços inválidos | Trim e validar |
| `"test@test.test"` | Email de teste | Marcar como suspeito (possível fraude) |

#### 1.2.5 Nomes Inválidos

**Tabelas**: `person.customer`, `humanresources.employee`

| Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-----------------|--------------|---------------------|
| `"João123"` | Números no nome | Validar alfabético + espaços |
| `"Maria###"` | Caracteres especiais | Normalizar ou rejeitar |
| `"Cliente Teste"` | Nome genérico | Detectar padrões de teste (regex) |
| `"XXXX"` | Placeholder | Marcar como inválido |
| `"   "` (apenas espaços) | String vazia disfarçada | Trim e validar não-vazio |
| `"A"` (1 caractere) | Nome muito curto | Validar comprimento mínimo |

---

### 1.3 Valores Acima do Normal (1-3%)

**Descrição**: Outliers estatísticos legítimos (não fraudes) para testar detecção de anomalias.

**Tabelas**: `sales.salesorder`, `purchasing.purchaseorderheader`, `warehouse.transaction_history`

| Tipo | Exemplo | Característica | Tratamento Esperado |
|------|---------|----------------|---------------------|
| Pedido de luxo | Pedido de R$ 150.000 (média: R$ 300) | Valor 500x acima da média | Alertar para revisão manual, não bloquear automaticamente |
| Compra empresarial | 10.000 unidades de canetas (média: 5 unidades) | Quantidade muito alta | Validar com histórico do cliente (B2B vs B2C) |
| Importação | Prazo de entrega de 120 dias (média: 7 dias) | Lead time extremo | Verificar se produto é importado |
| Frete internacional | Frete de R$ 5.000 para produto de R$ 200 | Frete > valor do produto | Validar modalidade de envio (aéreo express) |

**Cenários de Teste**:
- Detecção de outliers com Z-score ou IQR
- Análise contextual (cliente B2B vs B2C)
- Alertas de revisão manual sem bloqueio automático
- Modelos de ML para separar outliers legítimos de fraudes

---

## 2. Fraudes e Atividades Suspeitas

### 2.1 Fraudes em Compras (1-2%)

#### 2.1.1 Nome Real + CPF Falso

**Tabela**: `person.customer`, `sales.salesorder`

**Padrão**:
- Nome aparentemente legítimo: `"João da Silva"`
- CPF obviamente falso: `"000.000.000-00"`, `"123.456.789-00"`, `"111.111.111-11"`
- Endereço pode ser válido ou não

**Detecção**:
- Validar dígito verificador do CPF
- Bloquear sequências e padrões conhecidos
- Cruzar com base da Receita Federal (se disponível)

#### 2.1.2 Nome Falso + Endereço Real

**Tabela**: `person.customer`, `sales.salesorder`

**Padrão**:
- Nome genérico/falso: `"Cliente Teste"`, `"Fulano de Tal"`, `"João Silva"` (muito comum)
- Endereço real e conhecido (ex: shopping, condomínio)
- Entrega em locais com alto tráfego (dificulta rastreamento)

**Detecção**:
- Detectar nomes muito comuns ou genéricos (frequência na base)
- Cruzar endereço com bases de locais comerciais
- Alertar entregas em endereços com múltiplos destinatários diferentes

#### 2.1.3 Cartões Clonados (Compras Geograficamente Impossíveis)

**Tabela**: `sales.salesorder`, `sales.salesorder.payment_info`

**Padrão**:
```json
{
  "order_1": {
    "customer_id": 12345,
    "order_date": "2024-01-15T10:00:00",
    "shipping_city": "São Paulo",
    "shipping_state": "SP"
  },
  "order_2": {
    "customer_id": 12345,
    "order_date": "2024-01-15T10:05:00",  // 5 minutos depois
    "shipping_city": "Rio de Janeiro",
    "shipping_state": "RJ"
  }
}
```

**Detecção**:
- Calcular distância geográfica entre compras consecutivas
- Verificar tempo entre transações vs tempo de deslocamento físico
- Alertar compras em cidades diferentes no mesmo dia (mesmo customer_id)

#### 2.1.4 Múltiplas Compras com Cartões Diferentes no Mesmo Endereço

**Tabela**: `sales.salesorder`

**Padrão**:
- Endereço de entrega: `"Rua X, 123, Apto 45"`
- Customer 1: Cartão terminado em `1234`, data: `2024-01-10`
- Customer 2: Cartão terminado em `5678`, data: `2024-01-11`
- Customer 3: Cartão terminado em `9012`, data: `2024-01-12`

**Detecção**:
- Agrupar pedidos por endereço de entrega
- Contar customer_ids únicos no mesmo endereço em curto período (7 dias)
- Alertar quando > 3 clientes diferentes entregam no mesmo local

#### 2.1.5 Alto Valor + Dados Cadastrais Inconsistentes

**Tabela**: `sales.salesorder`, `person.customer`

**Padrão**:
- Pedido de R$ 10.000+
- Email gratuito: `joao123@gmail.com`
- Endereço de luxo: `"Av. Brigadeiro Faria Lima, 3000"` (região nobre)
- Cadastro recém-criado (< 24h)
- Sem histórico de compras

**Detecção**:
- Cruzar valor do pedido com "idade" da conta
- Detectar emails de provedores gratuitos em compras de alto valor
- Verificar consistência entre CEP (região nobre) e comportamento de compra

---

### 2.2 Cadastros Fraudulentos (1-3%)

#### 2.2.1 CPFs Sintéticos (Válidos Matematicamente, Inexistentes)

**Tabela**: `person.customer`

**Padrão**:
- CPF passa na validação de dígito verificador
- Mas não existe na base da Receita Federal
- Gerado por algoritmos públicos de geração de CPF

**Detecção**:
- Integrar com API da Receita Federal (quando disponível)
- Detectar padrões de CPFs gerados por ferramentas conhecidas
- Monitorar CPFs com data de criação recente e sem histórico

#### 2.2.2 Múltiplos Cadastros com Mesmo Telefone/Email

**Tabela**: `person.customer`, `person.customer_email`

**Padrão**:
```json
[
  {"customer_id": 1001, "cpf": "123.456.789-00", "email": "teste@email.com"},
  {"customer_id": 1002, "cpf": "987.654.321-00", "email": "teste@email.com"},
  {"customer_id": 1003, "cpf": "111.222.333-44", "email": "teste@email.com"}
]
```

**Detecção**:
- GROUP BY email ou telefone, contar CPFs distintos
- Alertar quando 1 email tem > 2 CPFs diferentes
- Permitir exceções (email corporativo compartilhado)

#### 2.2.3 Endereços Falsos/Inexistentes

**Tabela**: `person.address`

**Padrão**:
- `"Rua X, número 99999"` (número improvável)
- CEP válido mas rua inexistente naquele CEP
- Complemento genérico: `"Casa"`, `"Apto 1"`

**Detecção**:
- Validar CEP contra API dos Correios
- Verificar se logradouro corresponde ao CEP
- Alertar números de endereço > 9999 (improváveis)

#### 2.2.4 Combinações Impossíveis

**Tabela**: `person.customer`

**Padrão**:
- Nome brasileiro: `"José da Silva"`
- Telefone internacional sem explicação: `"+1 555-1234"` (EUA)
- CPF brasileiro mas endereço no exterior

**Detecção**:
- Cruzar país do telefone com país do CPF
- Validar consistência entre nacionalidade dos documentos
- Permitir exceções para expatriados (validar com histórico)

---

### 2.3 Processos de Compra Incompletos/Fantasmas (2-5%)

#### 2.3.1 Pedidos Criados mas Nunca Pagos

**Tabela**: `sales.salesorder`

**Padrão**:
```json
{
  "salesordernumber": "SO-123456789",
  "order_date": "2024-01-01T10:00:00",
  "order_status": "pending",
  "payment_info": {
    "transactions": [
      {"status": "pending", "method": "credit_card"}
    ]
  },
  "days_since_creation": 45  // > 30 dias
}
```

**Detecção**:
- Query: `WHERE order_status = 'pending' AND DATEDIFF(NOW(), order_date) > 30`
- Alertar para limpeza ou cancelamento automático
- Análise de taxa de conversão (carrinho abandonado)

#### 2.3.2 Pedidos Pagos mas Nunca Enviados

**Tabela**: `sales.salesorder`

**Padrão**:
```json
{
  "salesordernumber": "SO-987654321",
  "order_status": "paid",
  "payment_info": {"transactions": [{"status": "paid"}]},
  "shipping_status": null,  // ou "pending" há > 15 dias
  "days_since_payment": 20
}
```

**Detecção**:
- Query: `WHERE payment_status = 'paid' AND (shipping_status IS NULL OR shipping_status = 'pending') AND days_since_payment > 15`
- Alertar operações para investigação
- Indicador de problema no warehouse ou integração

#### 2.3.3 Pedidos Enviados mas Nunca Entregues

**Tabela**: `sales.salesorder`, `warehouse.transaction_history`

**Padrão**:
```json
{
  "salesordernumber": "SO-555555555",
  "shipping_status": "in_transit",
  "tracking_code": "BR123456789",
  "shipping_date": "2024-01-01",
  "days_in_transit": 120,  // > 90 dias
  "last_tracking_update": "2024-01-15"  // parado há meses
}
```

**Detecção**:
- Query: `WHERE shipping_status = 'in_transit' AND DATEDIFF(NOW(), shipping_date) > 90`
- Integrar com API de rastreamento (Correios, transportadoras)
- Alertar cliente e processar reembolso/reenvio

#### 2.3.4 Pedidos com Produtos Inexistentes

**Tabela**: `sales.salesorder.order_items`

**Padrão**:
```json
{
  "line_number": "SO-123456789-01",
  "product_id": 99999,  // ID que não existe em product.product
  "sku": "SKU-INEXISTENTE",
  "quantity": 1
}
```

**Detecção**:
- LEFT JOIN com `product.product` e verificar NULL
- Indicador de erro de sistema ou produto deletado após venda
- Alertar para correção de dados ou cancelamento da linha

#### 2.3.5 Disputas de Chargeback

**Tabela**: `sales.salesorder.payment_info`

**Padrão**:
```json
{
  "transactions": [
    {
      "status": "paid",
      "dispute_status": "open",
      "dispute_reason": "fraudulent",
      "dispute_date": "2024-02-01"
    }
  ]
}
```

**Detecção**:
- Query: `WHERE payment_info->'transactions'->0->>'dispute_status' = 'open'`
- Alertar financeiro e operações
- Bloquear novos pedidos do cliente até resolução

---

### 2.4 Fraudes em Purchasing (1-2%)

#### 2.4.1 Ordens de Compra Duplicadas

**Tabela**: `purchasing.purchaseorderheader`

**Padrão**:
```json
[
  {
    "purchaseorderheader_id": "PO-111111111",
    "vendor_id": 5,
    "order_date": "2024-01-10",
    "total_due": 50000.00
  },
  {
    "purchaseorderheader_id": "PO-222222222",
    "vendor_id": 5,
    "order_date": "2024-01-11",  // 1 dia depois
    "total_due": 50000.00  // mesmo valor
  }
]
```

**Detecção**:
- GROUP BY vendor_id, total_due com diferença de datas < 7 dias
- Alertar para revisão (pode ser erro ou suborno)
- Verificar se itens comprados são os mesmos

#### 2.4.2 Fornecedores Fantasmas

**Tabela**: `purchasing.vendor`

**Padrão**:
- CNPJ válido matematicamente
- Empresa sem atividade econômica (consultoria Receita Federal)
- Endereço genérico ou inexistente
- Sem notas fiscais emitidas nos últimos 12 meses

**Detecção**:
- Integrar com API Receita Federal (situação cadastral)
- Verificar emissão de NF-e (SEFAZ)
- Auditar fornecedores sem histórico de entregas

#### 2.4.3 Preços Superfaturados

**Tabela**: `purchasing.purchaseorderdetail`

**Padrão**:
- Produto X: preço médio histórico R$ 100
- Nova ordem: preço R$ 500 (5x acima)
- Fornecedor novo ou sem histórico

**Detecção**:
- Calcular média e desvio padrão por produto
- Alertar preços > 3 desvios padrão acima da média
- Exigir aprovação de nível superior (workflow)

#### 2.4.4 Aprovações Sem Autorização

**Tabela**: `purchasing.purchaseorderheader`

**Padrão**:
```json
{
  "purchaseorderheader_id": "PO-123456789",
  "employee_id": 99,  // Funcionário sem permissão de aprovar
  "total_due": 100000.00,  // Valor acima do limite do funcionário
  "status": "approved"
}
```

**Detecção**:
- Cruzar employee_id com tabela de permissões/alçadas
- Validar valor do pedido vs limite de aprovação do funcionário
- Bloquear aprovações automáticas fora da alçada

#### 2.4.5 Entregas em Endereços Pessoais

**Tabela**: `purchasing.purchaseorderheader`, `warehouse.warehouselocation`

**Padrão**:
```json
{
  "purchaseorderheader_id": "PO-987654321",
  "shipping_address": "Rua Residencial, 123, Casa",  // Não é warehouse
  "employee_id": 50  // Funcionário que aprovou o pedido
}
```

**Detecção**:
- Validar se endereço de entrega está em `warehouse.warehouselocation`
- Alertar entregas em endereços não cadastrados como warehouses
- Cruzar endereço de entrega com endereço do funcionário aprovador

---

## 3. Anomalias em Warehouse (Logística e Estoque)

### 3.1 Transferências Perdidas/Fantasmas (2-5%)

#### 3.1.1 Transferências Nunca Concluídas

**Tabela**: `warehouse.transaction_history`

**Padrão**:
```json
{
  "transaction_id": "TR-123456789",
  "transactiontype_id": 3,  // Transferência
  "traceability_info": {
    "source_warehouselocation_id": 10,
    "target_warehouselocation_id": 20,
    "status": "in_transit"
  },
  "transaction_date": "2024-01-01",
  "days_in_transit": 95  // > 90 dias
}
```

**Detecção**:
- Query: `WHERE status = 'in_transit' AND DATEDIFF(NOW(), transaction_date) > 90`
- Alertar para investigação (perda, roubo, erro de sistema)
- Ajustar inventário se confirmada perda

#### 3.1.2 Quantidade Recebida ≠ Quantidade Enviada

**Tabela**: `warehouse.transaction_history`

**Padrão**:
```json
{
  "transaction_send": {
    "transaction_id": "TR-111111111",
    "quantity": 100,
    "source_warehouselocation_id": 10
  },
  "transaction_receive": {
    "transaction_id": "TR-111111111-RCV",
    "quantity": 95,  // Faltam 5 unidades
    "target_warehouselocation_id": 20,
    "traceability_info": {
      "quality_check_status": "pass",
      "notes": "5 unidades danificadas no transporte"
    }
  }
}
```

**Detecção**:
- Cruzar transações de envio com recebimento (mesmo reference_order_id)
- Calcular divergência: `(received - sent) / sent * 100`
- Alertar se divergência > 5%

#### 3.1.3 Transferências para o Mesmo Local

**Tabela**: `warehouse.transaction_history`

**Padrão**:
```json
{
  "transaction_id": "TR-999999999",
  "transactiontype_id": 3,
  "traceability_info": {
    "source_warehouselocation_id": 10,
    "target_warehouselocation_id": 10  // MESMO local!
  }
}
```

**Detecção**:
- Query: `WHERE source_warehouselocation_id = target_warehouselocation_id`
- Indicador de erro de sistema ou má configuração
- Bloquear transferências com origem = destino na API

#### 3.1.4 Transações Sem Rastreabilidade

**Tabela**: `warehouse.transaction_history`

**Padrão**:
```json
{
  "transaction_id": "TR-888888888",
  "quantity": 50,
  "traceability_info": null  // Sem rastreamento!
}
```

**Detecção**:
- Query: `WHERE traceability_info IS NULL`
- Exigir traceability_info obrigatório em transações críticas
- Alertar para auditoria (possível fraude ou erro)

---

### 3.2 Inventário com Divergências (3-8%)

#### 3.2.1 Quantidade Física ≠ Quantidade no Sistema

**Tabela**: `warehouse.product_inventory`

**Padrão**:
```json
{
  "product_id": 123,
  "warehouselocation_id": 10,
  "quantity_on_hand": 100,  // Sistema
  "physical_count": 95,     // Contagem física
  "last_cycle_count": "2024-01-15",
  "variance": -5
}
```

**Detecção**:
- Implementar contagens cíclicas (cycle counts)
- Calcular: `variance = physical_count - quantity_on_hand`
- Alertar se |variance| > 5% ou valor absoluto > 10 unidades

#### 3.2.2 Produtos com Custo Negativo ou Zero

**Tabela**: `warehouse.product_inventory`, `warehouse.transaction_history`

**Padrão**:
```json
{
  "product_id": 456,
  "actual_cost": -10.50  // Negativo!
}
```

**Detecção**:
- Query: `WHERE actual_cost <= 0`
- Indicador de erro de sistema, estorno mal feito ou bug
- Bloquear custos negativos na API (validação Pydantic)

#### 3.2.3 Produtos Disponíveis Sem Localização Física

**Tabela**: `warehouse.product_inventory`

**Padrão**:
```json
{
  "product_id": 789,
  "quantity_on_hand": 50,
  "warehouselocation_id": null  // Sem localização!
}
```

**Detecção**:
- Query: `WHERE quantity_on_hand > 0 AND warehouselocation_id IS NULL`
- Impede picking (operador não sabe onde buscar)
- Exigir localização física antes de marcar como disponível

#### 3.2.4 Estoque Negativo (Overselling)

**Tabela**: `warehouse.product_inventory`

**Padrão**:
```json
{
  "product_id": 101,
  "quantity_on_hand": -10  // Negativo!
}
```

**Detecção**:
- Query: `WHERE quantity_on_hand < 0`
- Indicador de overselling (vendeu mais do que tinha)
- Implementar validação no momento da venda (ATP - Available to Promise)

#### 3.2.5 Movimentações Sem Ordem de Origem

**Tabela**: `warehouse.transaction_history`

**Padrão**:
```json
{
  "transaction_id": "TR-777777777",
  "transactiontype_id": 2,  // Saída para venda
  "quantity": 10,
  "reference_order_id": null  // Sem ordem de venda vinculada!
}
```

**Detecção**:
- Query: `WHERE transactiontype_id = 2 AND reference_order_id IS NULL`
- Indicador de movimentação não autorizada (possível roubo)
- Exigir ordem de origem para transações de saída

---

### 3.3 Problemas de Qualidade (1-2%)

#### 3.3.1 Produtos Reprovados mas Enviados

**Tabela**: `warehouse.transaction_history`, `sales.salesorder`

**Padrão**:
```json
{
  "transaction_id": "TR-666666666",
  "product_id": 202,
  "traceability_info": {
    "quality_check_status": "fail"  // Reprovado!
  },
  "reference_order_id": 12345  // Mas foi vendido!
}
```

**Detecção**:
- Query: `WHERE quality_check_status = 'fail' AND reference_order_id IS NOT NULL`
- Indicador de erro de processo ou fraude
- Bloquear envio de produtos com status != "pass"

#### 3.3.2 Quebra de Cold Chain (IoT)

**Tabela**: `warehouse.transaction_history.traceability_info.iot_sensor_data`

**Padrão**:
```json
{
  "product_id": 303,
  "product_category": "Alimentos Refrigerados",
  "temp_at_scan": 15.5,  // Temperatura OK: 2-8°C esperado
  "expected_range": {"min": 2.0, "max": 8.0},
  "iot_sensor_data": {
    "temp_at_scan": 15.5,  // FORA DO RANGE!
    "device_id": "IOT-SCANNER-123"
  }
}
```

**Detecção**:
- Definir ranges de temperatura por categoria de produto
- Query: `WHERE temp_at_scan NOT BETWEEN min_temp AND max_temp`
- Alertar e bloquear envio de produtos com temperatura fora do range

#### 3.3.3 Produtos Vencidos em Estoque Ativo

**Tabela**: `warehouse.product_inventory`, `product.product`

**Padrão**:
```json
{
  "product_id": 404,
  "expiration_date": "2023-12-31",  // Vencido!
  "quantity_on_hand": 20,
  "status": "available"  // Ainda disponível para venda!
}
```

**Detecção**:
- Query: `WHERE expiration_date < NOW() AND status = 'available'`
- Implementar rotina de FEFO (First Expired, First Out)
- Alertar para descarte ou bloqueio de venda

---

### 3.4 Valores Negativos (2-4%)

**Tabela**: `warehouse.transaction_history`, `sales.salesorder`, `purchasing.purchaseorderheader`

| Campo | Padrão Inválido | Tipo de Erro | Tratamento Esperado |
|-------|-----------------|--------------|---------------------|
| `actual_cost` | `-50.00` | Estorno mal feito | Investigar transação de estorno, ajustar custo |
| `quantity` | `-10` | Bug de integração | Bloquear quantidades negativas na API |
| `freight` | `-100.00` | Ajuste de sistema mal implementado | Validar lógica de desconto de frete |
| `total_due` | `-500.00` | Erro de cálculo | Recalcular total, validar regras de negócio |
| `tax_amt` | `-18.00` | Imposto negativo (impossível) | Bloquear na validação Pydantic |

**Detecção**:
- Query: `WHERE actual_cost < 0 OR quantity < 0 OR freight < 0 OR total_due < 0`
- Implementar validação `ge=0` (greater than or equal) em schemas Pydantic
- Permitir valores negativos apenas em tabelas de ajustes/estornos (com auditoria)

---

## 4. Implementação nos Seeders

### 4.1 Função Genérica de Chaos Engineering

```python
import random
from typing import List, Dict, Any
from datetime import datetime, timedelta

def apply_data_quality_chaos(
    batch: List[Dict[str, Any]],
    chaos_rate: float = 0.05,
    allowed_chaos_types: List[str] = None
) -> List[Dict[str, Any]]:
    """
    Aplica chaos engineering (5% padrão) nos dados gerados.

    Args:
        batch: Lista de dicionários com dados a serem inseridos
        chaos_rate: Percentual de registros a serem afetados (0.01 a 0.10)
        allowed_chaos_types: Lista de tipos de chaos permitidos (None = todos)

    Returns:
        Lista de dicionários com anomalias injetadas
    """
    if not batch:
        return batch

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    all_chaos_types = [
        "duplicate", "dirty_cpf", "dirty_phone", "dirty_email",
        "dirty_cep", "dirty_name", "outlier_value", "outlier_quantity",
        "fraud_fake_cpf", "fraud_fake_address", "fraud_cloned_card",
        "fraud_duplicate_order", "fraud_phantom_vendor",
        "incomplete_payment", "incomplete_shipping", "incomplete_delivery",
        "warehouse_lost_transfer", "warehouse_quantity_mismatch",
        "warehouse_negative_stock", "warehouse_missing_location",
        "warehouse_quality_fail", "warehouse_cold_chain_break",
        "negative_values", "missing_refs"
    ]

    chaos_types = allowed_chaos_types if allowed_chaos_types else all_chaos_types

    for idx in chaos_indices:
        chaos_type = random.choice(chaos_types)

        # Duplicidade
        if chaos_type == "duplicate" and "customer_name" in batch[idx]:
            duplicated = batch[idx].copy()
            duplicated["customer_name"] = duplicated["customer_name"].upper()
            duplicated["row_guid"] = uuid.uuid4()
            batch.append(duplicated)

        # CPF falso
        elif chaos_type == "dirty_cpf" and "cpf" in batch[idx]:
            batch[idx]["cpf"] = random.choice([
                "000.000.000-00", "111.111.111-11", "123.456.789-00"
            ])

        # Telefone inválido
        elif chaos_type == "dirty_phone" and "phone" in batch[idx]:
            batch[idx]["phone"] = random.choice([
                "0000000", "11111111111", "(00) 0000-0000", "99 99999-9999"
            ])

        # Email inválido
        elif chaos_type == "dirty_email" and "email" in batch[idx]:
            batch[idx]["email"] = random.choice([
                "email@dominio", "@dominio.com", "email@@dominio.com", "test@test.test"
            ])

        # CEP inválido
        elif chaos_type == "dirty_cep" and "cep" in batch[idx]:
            batch[idx]["cep"] = random.choice([
                "00000-000", "XXXXX-XXX", "12345-678"
            ])

        # Nome inválido
        elif chaos_type == "dirty_name" and "customer_name" in batch[idx]:
            batch[idx]["customer_name"] = random.choice([
                "João123", "Maria###", "Cliente Teste", "XXXX", "   ", "A"
            ])

        # Outlier de valor
        elif chaos_type == "outlier_value" and "total_amount" in batch[idx]:
            current_value = batch[idx]["total_amount"]
            batch[idx]["total_amount"] = current_value * random.randint(5, 10)

        # Outlier de quantidade
        elif chaos_type == "outlier_quantity" and "quantity" in batch[idx]:
            batch[idx]["quantity"] = random.randint(1000, 10000)

        # Fraude: CPF falso com nome real
        elif chaos_type == "fraud_fake_cpf" and "cpf" in batch[idx]:
            batch[idx]["cpf"] = "000.000.000-00"
            # Nome permanece realista

        # Fraude: Nome falso com endereço real
        elif chaos_type == "fraud_fake_address" and "customer_name" in batch[idx]:
            batch[idx]["customer_name"] = "Cliente Teste"
            # Endereço permanece válido

        # Fraude: Cartão clonado (compras impossíveis geograficamente)
        elif chaos_type == "fraud_cloned_card" and "shipping_city" in batch[idx]:
            batch[idx]["shipping_city"] = "São Paulo"
            batch[idx]["billing_city"] = "Rio de Janeiro"
            batch[idx]["order_date"] = datetime.now()

        # Fraude: Ordem de compra duplicada
        elif chaos_type == "fraud_duplicate_order" and "purchaseorderheader_id" in batch[idx]:
            duplicated = batch[idx].copy()
            duplicated["purchaseorderheader_id"] = f"PO-{random.randint(100000000, 999999999)}"
            duplicated["order_date"] = batch[idx]["order_date"] + timedelta(days=1)
            duplicated["row_guid"] = uuid.uuid4()
            batch.append(duplicated)

        # Processo incompleto: Pagamento pendente há muito tempo
        elif chaos_type == "incomplete_payment" and "order_status" in batch[idx]:
            batch[idx]["order_status"] = "pending"
            batch[idx]["order_date"] = datetime.now() - timedelta(days=45)

        # Processo incompleto: Pago mas não enviado
        elif chaos_type == "incomplete_shipping" and "payment_info" in batch[idx]:
            batch[idx]["payment_info"]["transactions"][0]["status"] = "paid"
            batch[idx]["shipping_status"] = None
            batch[idx]["order_date"] = datetime.now() - timedelta(days=20)

        # Warehouse: Transferência perdida
        elif chaos_type == "warehouse_lost_transfer" and "transaction_date" in batch[idx]:
            batch[idx]["transaction_date"] = datetime.now() - timedelta(days=95)
            if "traceability_info" in batch[idx]:
                batch[idx]["traceability_info"]["status"] = "in_transit"

        # Warehouse: Estoque negativo
        elif chaos_type == "warehouse_negative_stock" and "quantity" in batch[idx]:
            batch[idx]["quantity"] = random.randint(-50, -1)

        # Warehouse: Sem localização física
        elif chaos_type == "warehouse_missing_location" and "warehouselocation_id" in batch[idx]:
            batch[idx]["warehouselocation_id"] = None

        # Warehouse: Qualidade reprovada mas enviado
        elif chaos_type == "warehouse_quality_fail" and "traceability_info" in batch[idx]:
            batch[idx]["traceability_info"]["quality_check_status"] = "fail"
            # Mas reference_order_id permanece preenchido (produto foi vendido)

        # Warehouse: Cold chain quebrada
        elif chaos_type == "warehouse_cold_chain_break" and "traceability_info" in batch[idx]:
            if "iot_sensor_data" in batch[idx]["traceability_info"]:
                batch[idx]["traceability_info"]["iot_sensor_data"]["temp_at_scan"] = random.uniform(15.0, 25.0)

        # Valores negativos
        elif chaos_type == "negative_values":
            if "actual_cost" in batch[idx]:
                batch[idx]["actual_cost"] = -abs(batch[idx]["actual_cost"])
            elif "total_due" in batch[idx]:
                batch[idx]["total_due"] = -abs(batch[idx]["total_due"])
            elif "freight" in batch[idx]:
                batch[idx]["freight"] = -abs(batch[idx]["freight"])

        # Referências faltando
        elif chaos_type == "missing_refs":
            if "reference_order_id" in batch[idx]:
                batch[idx]["reference_order_id"] = None
            elif "vendor_id" in batch[idx]:
                batch[idx]["vendor_id"] = None

    return batch
```

### 4.2 Exemplo de Uso em Seeder

```python
# Em dbapi/api/seeder/sales/sales_order.py

async def seed_sales_orders(count: int = 50):
    async with AsyncSessionLocal() as session:
        # 1. Buscar FKs
        customer_ids = (await session.execute(select(CustomerModel.customer_id))).scalars().all()
        # ... outras FKs

        # 2. Gerar dados limpos
        orders = generate_sales_order_payloads(count, customer_ids, channel_ids, offer_ids, seller_ids, product_ids)

        # 3. Aplicar chaos engineering (5% de anomalias)
        orders_with_chaos = apply_data_quality_chaos(
            orders,
            chaos_rate=0.05,
            allowed_chaos_types=[
                "duplicate", "dirty_cpf", "fraud_fake_cpf",
                "fraud_cloned_card", "incomplete_payment", "negative_values"
            ]
        )

        # 4. Inserir em lote
        stmt = insert(SalesOrderModel).values(orders_with_chaos)
        await session.execute(stmt)
        await session.commit()

        print(f"✅ {len(orders_with_chaos)} pedidos semeados (incluindo {int(len(orders_with_chaos) * 0.05)} com anomalias)")
```

---

## 5. Metadados e Documentação

Para cada tabela que recebe dados com chaos engineering, documentar:

1. **Tipos de anomalias injetadas** (quais chaos_types estão ativos)
2. **Percentual de anomalias** (chaos_rate configurado)
3. **Queries de detecção** (SQL para identificar cada tipo de anomalia)
4. **Cenários de teste** (pipelines de limpeza esperados)

### Exemplo de Metadados (JSON)

```json
{
  "table": "sales.salesorder",
  "chaos_engineering": {
    "enabled": true,
    "chaos_rate": 0.05,
    "chaos_types": [
      {
        "type": "duplicate",
        "description": "Registros duplicados com variação de case",
        "detection_query": "SELECT customer_id, UPPER(customer_name) as name_upper, COUNT(*) FROM sales.salesorder GROUP BY customer_id, name_upper HAVING COUNT(*) > 1",
        "expected_count": "~2-5% of total records"
      },
      {
        "type": "fraud_cloned_card",
        "description": "Compras geograficamente impossíveis no mesmo dia",
        "detection_query": "WITH orders AS (SELECT customer_id, order_date::date, shipping_city, LAG(shipping_city) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_city FROM sales.salesorder) SELECT * FROM orders WHERE shipping_city != prev_city AND order_date = prev_order_date",
        "expected_count": "~1-2% of total records"
      }
    ]
  }
}
```

---

## 6. Queries de Detecção para Data Quality Dashboards

### 6.1 Dashboard de Qualidade de Dados

```sql
-- Total de registros com CPF inválido
SELECT
    'Invalid CPF' as issue_type,
    COUNT(*) as issue_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM person.customer) as percentage
FROM person.customer
WHERE cpf IN ('000.000.000-00', '111.111.111-11', '123.456.789-00');

-- Registros duplicados (case-insensitive)
SELECT
    'Duplicates' as issue_type,
    COUNT(*) as issue_count
FROM (
    SELECT UPPER(customer_name), COUNT(*) as cnt
    FROM person.customer
    GROUP BY UPPER(customer_name)
    HAVING COUNT(*) > 1
) subq;

-- Estoque negativo
SELECT
    'Negative Stock' as issue_type,
    COUNT(*) as issue_count
FROM warehouse.product_inventory
WHERE quantity_on_hand < 0;

-- Pedidos pagos mas não enviados (> 15 dias)
SELECT
    'Unpaid Orders' as issue_type,
    COUNT(*) as issue_count
FROM sales.salesorder
WHERE order_status = 'paid'
  AND shipping_status IS NULL
  AND order_date < NOW() - INTERVAL '15 days';
```

### 6.2 Dashboard de Detecção de Fraudes

```sql
-- Múltiplos cadastros com mesmo email
SELECT
    email,
    COUNT(DISTINCT cpf) as distinct_cpfs,
    COUNT(*) as total_accounts
FROM person.customer_email
GROUP BY email
HAVING COUNT(DISTINCT cpf) > 2;

-- Compras geograficamente impossíveis
WITH order_pairs AS (
    SELECT
        customer_id,
        order_date,
        shipping_city,
        LAG(shipping_city) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_city,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) as prev_date
    FROM sales.salesorder
)
SELECT *
FROM order_pairs
WHERE shipping_city != prev_city
  AND order_date::date = prev_date::date;

-- Fornecedores fantasmas (sem entregas nos últimos 90 dias)
SELECT
    v.vendor_id,
    v.vendor_name,
    MAX(poh.order_date) as last_order_date,
    DATEDIFF('day', MAX(poh.order_date), NOW()) as days_since_last_order
FROM purchasing.vendor v
LEFT JOIN purchasing.purchaseorderheader poh ON v.vendor_id = poh.vendor_id
GROUP BY v.vendor_id, v.vendor_name
HAVING MAX(poh.order_date) < NOW() - INTERVAL '90 days'
   OR MAX(poh.order_date) IS NULL;
```

---

## 7. Próximos Passos

1. **Implementar `apply_data_quality_chaos()` em módulo utilitário**:
   - Criar `dbapi/api/utils/chaos_engineering.py` com a função genérica
   - Importar em todos os seeders dos schemas críticos

2. **Atualizar seeders existentes**:
   - `sales/sales_order.py`
   - `purchasing/purchase_purchaseorderheader.py`
   - `warehouse/transaction_history.py`
   - `person/person_customer.py`

3. **Criar validações no product seeder**:
   - Verificar contagem antes de inserir (limite 100k)
   - Adicionar argparse para controlar chaos_rate

4. **Implementar dashboards de Data Quality**:
   - Integrar com Trino para queries analíticas
   - Criar views materializadas com métricas de qualidade
   - Alertas via Airflow quando anomalias excedem thresholds

5. **Documentar metadados de chaos**:
   - Criar arquivo JSON por tabela em `dbapi/api/seeder/metadata/`
   - Consumir metadados em pipelines de validação (Great Expectations)

6. **Testar pipelines de limpeza**:
   - Implementar dbt models para normalização
   - Criar testes unitários para detecção de fraudes
   - Validar recovery de dados após limpeza

---

## Conclusão

Este documento serve como **fonte única de verdade** para todos os tipos de anomalias injetadas nos dados. Deve ser mantido atualizado sempre que novos tipos de chaos forem adicionados aos seeders.

**Importante**: Esses dados "sujos" são **intencionais e documentados**, não são bugs! Eles existem para treinar pipelines de limpeza e detecção de fraudes em um ambiente controlado.
