# Chaos Engineering Documentation — Data Quality Anomalies

Este documento descreve todas as anomalias intencionalmente injetadas nos dados pelos seeders para simular cenários reais de engenharia de dados.

## Objetivo

Criar um ambiente de testes realista que permite:
1. Desenvolver e testar pipelines de limpeza de dados
2. Treinar modelos de detecção de fraudes
3. Validar regras de qualidade de dados
4. Preparar analistas para cenários reais de produção

## Taxa de Anomalias

Todos os seeders dos schemas `sales`, `purchasing`, `warehouse`, `person`, `humanresources`, `marketing` injetam **entre 1% e 10%** de anomalias, distribuídas conforme especificado abaixo.

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

## 3.5 Anomalias em Recursos Humanos (1-3%)

**Contexto importante**: diferente de `sales`/`purchasing`/`warehouse`, o seeder de `humanresources.employee` (`dbapi/api/seeder/humanresources/humanresources_employee.py`) hoje **não gera histórico real** — todo registro nasce com `is_current=True` e nunca é fechado por um `MERGE`/upsert que compare com a versão anterior. Isso significa que a Camada Silver **não pode** corrigir estas anomalias olhando para "o valor anterior do mesmo funcionário" (não existe). As estratégias de correção abaixo usam, em vez disso: (a) dados de referência/dimensão (ex. `humanresources.jobrole`, que tem seu próprio SCD2 com `min_salary`/`max_salary`), (b) estatística de pares (mediana do mesmo cargo/departamento) ou (c) regras determinísticas de negócio — nunca "último valor conhecido do indivíduo".

### 3.5.1 Salário Fora da Faixa do Cargo

**Tabelas**: `humanresources.employee` (`current_salary`) + `humanresources.jobrole` (`min_salary`, `max_salary`, `is_current=true`)

**Padrão**:
```json
{
  "employee_id": 42,
  "job_role_id": 7,
  "current_salary": 55000.00,
  "jobrole_range": {"min_salary": 8000.00, "max_salary": 22000.00}
}
```

**Detecção**:
```sql
SELECT e.employee_id, e.current_salary, j.min_salary, j.max_salary
FROM humanresources.employee e
JOIN humanresources.jobrole j
  ON e.job_role_id = j.jobrole_id AND j.is_current = true
WHERE e.current_salary NOT BETWEEN j.min_salary AND j.max_salary;
```

**Correção em Silver (sem histórico do indivíduo)**:
- **Quarentena, não sobrescrita silenciosa**: adicionar coluna derivada `salary_flag` (`'above_range'` / `'below_range'` / `'ok'`). Salário é dado sensível — a correção definitiva é decisão de RH, não do pipeline.
- Se a Gold precisar de um valor "limpo" para métricas agregadas, gerar `current_salary_corrected = LEAST(GREATEST(current_salary, min_salary), max_salary)` **em coluna separada**, preservando `current_salary` original para auditoria.
- Alternativa estatística: imputar pela **mediana salarial de pares** (`PERCENTILE_CONT(0.5) ... PARTITION BY job_role_id` sobre os registros que já estão dentro da faixa), em vez de um valor fixo.

### 3.5.2 Vendedor Sem Comissão

**Tabela**: `humanresources.employee` (`role_attributes` JSONB, quando o cargo é de Vendas)

**Nota sobre o schema**: `role_attributes: Union[SalesRoleAttributes, ..., Dict[str, Any]]` tem fallback para `Dict[str, Any]`. Isso é o que permite o payload chaos (faltando `comission_pct`) entrar no Bronze sem quebrar a validação Pydantic — o fallback deve ser mantido de propósito para este cenário.

**Padrão**: `role_attributes->>'comission_pct' IS NULL OR = 0` com `job_title LIKE '%Vendas%'` / `%Sales%`.

**Correção em Silver**: mesmo princípio já usado no pipeline legado do AdventureWorks (`spark/jobs/silver_layer_advworks_scripts/advdim_salesperson.py::calculateSalesPersonCommission`) — backfill por **tabela/dict de comissão padrão por território ou tier de cargo** (ex. `{"North America": 0.05, "Europe": 0.04, "Pacific": 0.04}`), nunca por valor anterior do funcionário.

### 3.5.3 Vendedor Sem Território/Região

**Tabela**: `humanresources.employee` (`role_attributes.territory_codes`)

**Diferença em relação ao AdventureWorks**: lá existe uma dimensão própria de território (`territoryid`); no dbapi não há — o vendedor só carrega `country_id`/`state_province_id`.

**Correção em Silver**: backfill determinístico usando a geografia que já existe no próprio registro (`country_id` + `state_province_id`), com um mapeamento estático país/estado → território (mesmo padrão de `transformSalesPersonTerritory`, mas chaveado por geografia em vez de string de cargo).

### 3.5.4 Hierarquia Inconsistente (Manager em Departamento Diferente)

**Tabela**: `humanresources.employee` (`manager_id`, `department_id`)

**Padrão**: `manager_id` aponta para um funcionário de `department_id` diferente do subordinado.

**Detecção**: self-join de `employee` comparando `department_id` do funcionário com o do seu `manager_id`.

**Correção**: apenas sinalizar (`hierarchy_flag`) para revisão manual — reporte matricial pode ser legítimo, não deve ser "corrigido" automaticamente.

### 3.5.5 SCD2 Quebrado — Múltiplos "Atuais" ou Buracos Temporais

**Tabela**: `humanresources.employee`

**Padrão**: mais de um registro com `is_current=true` para o mesmo `employee_id`, ou `effective_end_date` de uma versão que não bate com o `effective_start_date` da próxima.

**Detecção**:
```sql
-- Mais de uma versão "atual"
SELECT employee_id, COUNT(*)
FROM humanresources.employee
WHERE is_current = true
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

**Correção**: esta é a única regra da seção que **constrói histórico em vez de depender dele** — em Silver, usar `ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_start_date DESC)` para manter apenas a versão mais recente como `is_current=true` e fechar as demais (`effective_end_date = próximo effective_start_date`). Recomendação estrutural: mover essa lógica de "fechar versão anterior" para o **processo de carga** (upsert/MERGE por `employee_id` comparando atributos rastreados), não só para a limpeza — é isso que resolve a causa raiz de "não existe histórico" mencionada acima.

### 3.5.6 Funcionário Desligado Ainda Marcado como Ativo

**Tabela**: `humanresources.employee`

**Padrão**: `end_date` preenchido (funcionário saiu) mas `is_active=true` e/ou `is_current=true`.

**Detecção**: `WHERE end_date IS NOT NULL AND (is_active = true OR is_current = true)`

**Correção**: regra determinística (não depende de histórico) — Silver fecha o registro automaticamente: `is_active=false`, `is_current=false`, `effective_end_date=end_date`.

---

## 3.6 Anomalias em Marketing (1-5%)

**Contexto importante — o Guarda Pydantic**: diferente de `sales`/`purchasing`/`warehouse`/`humanresources` (onde os seeders inserem `dict` cru via SQLAlchemy), os 4 seeders de `marketing` (`marketing_campaign.py`, `marketing_adgroup.py`, `marketing_ads.py`, `marketing_daily_performance.py`) **validam o payload pelo schema Pydantic antes do insert** (`Model(**payload).model_dump()`). Isso significa que **nem todo chaos passa direto** — cada regra abaixo indica se precisa contornar a validação (bypass) ou se já passa normalmente, porque o schema atual não cobre aquele cenário.

| Regra | Precisa bypass do Pydantic? | Motivo |
|---|---|---|
| 3.6.1 Naming convention quebrado | Não | `campaign_name`/`utm_campaign` são `str` livre, sem regex |
| 3.6.2 Budget zerado + status ativo | Não | `total_budget: Decimal = Field(ge=0.0)` aceita `0.0` |
| 3.6.3 Budget estourado (300%) | Não* | não é validado no schema da campanha; é uma checagem cross-table (campanha × soma de `daily_performance.cost`) |
| 3.6.4 Targeting vazio `{}` | **Sim** | `device_types: List[DeviceType]` é obrigatório, sem default |
| 3.6.5 Idade invertida (min > max) | **Sim** | bloqueado por `model_validator` em `AgeRange`, não por `Field` — "afrouxar" campos não resolve, precisa pular o validator ou o schema inteiro |
| 3.6.6 Links quebrados | **Sim** | `final_url: HttpUrl` e `MediaAsset.url: AnyUrl` são estritos |
| 3.6.7 Media type incompatível com extensão | Não | não existe validação cruzada `media_type` × extensão hoje |
| 3.6.8 Vazamento de campo entre plataformas | Não | `MarketingMetrics` aceita qualquer combinação de campos opcionais |
| 3.6.9 Clicks > Impressions | Não | só há `ge=0` individual em cada campo |
| 3.6.10 Custo zero com conversão | Não | idem |
| 3.6.11 Métricas retroativas (revisão) | Não** | não é um problema de schema; é o mesmo `(ad_id, date_ref)` inserido 2x com valores diferentes — precisa que o loader permita reprocessamento |
| 3.6.12 Funil de vídeo não-monótono | Não | nenhuma checagem de ordem entre `video_views_25/50/75/100` |
| 3.6.13 Reach > Impressions | Não | idem, sem checagem cruzada |
| 3.6.14 UTM duplicado entre campanhas | Não | `utm_campaign` não tem constraint de unicidade |

\* precisa existir performance já semeada para a campanha (ordem de execução: campaign → adgroup → ads → daily_performance).
\*\* precisa que o processo de carga permita inserir uma nova linha para uma chave `(ad_id, date_ref)` já existente (hoje o seeder só faz `insert()`, sem `ON CONFLICT`/upsert e sem constraint única — rodar o seeder 2x já duplicaria dias sem querer).

### 3.6.1 Quebra de Naming Convention

**Tabela**: `marketing.campaign` (`campaign_name`, `utm_campaign`)

**Padrão**: 5% das campanhas fogem do padrão `{PLATAFORMA}_{OBJETIVO}_{VARIANTE}` (ex. `GSN_Brand_Retargeting`) e vêm como texto livre de operador humano: `"Cópia de campanha teste 1"`, `"nova campanha (1)"`.

**Detecção**: `WHERE campaign_name !~ '^[A-Z]+_[A-Za-z]+_[A-Za-z]+$'` — testa se os scripts de extração de UTM por regex sobrevivem a nome fora do padrão.

**Correção em Silver**: não tentar "adivinhar" o UTM a partir do nome livre — extrair de `utm_campaign` (campo estruturado) quando disponível; se também estiver ausente/malformado, marcar `attribution_flag = 'unparsable_name'` em vez de forçar um regex genérico.

### 3.6.2 Budget Zerado com Status Ativo

**Tabela**: `marketing.campaign`

**Padrão**: `total_budget = 0.00` com `status = 'active'`.

**Detecção**: `WHERE total_budget = 0 AND status = 'active'`

**Correção**: sinalizar (`budget_flag = 'zero_budget_active'`) para revisão — pode ser campanha pausada na prática, mas com status desatualizado na API (comum quando a integração não sincroniza status em tempo real).

### 3.6.3 Budget Estourado (Overspend)

**Tabela**: `marketing.campaign` + `marketing.daily_performance` (soma de `cost`)

**Padrão**: para ~5% das campanhas, a soma de `cost` na `daily_performance` ultrapassa `total_budget` em até 300%.

**Detecção**:
```sql
SELECT c.campaign_id, c.total_budget, SUM(p.cost) as total_spent,
       (SUM(p.cost) / NULLIF(c.total_budget, 0)) as spend_ratio
FROM marketing.campaign c
JOIN marketing.daily_performance p ON p.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.total_budget
HAVING SUM(p.cost) > c.total_budget * 1.5;
```

**Correção**: nunca truncar o `cost` real — é dado financeiro da plataforma de anúncios. Apenas sinalizar (`overspend_flag`) e, se necessário para a Gold, expor `budget_utilization_pct` como métrica derivada.

### 3.6.4 Targeting Ausente/Vazio

**Tabela**: `marketing.adgroup` (`targeting_criteria`)

**Padrão**: `targeting_criteria = {}` em 10% dos AdGroups, como se a integração não tivesse permissão para ler dados de audiência daquela conta.

**Detecção**: `WHERE targeting_criteria = '{}'::jsonb OR targeting_criteria->'device_types' IS NULL`

**Correção**: não inventar critério de targeting — preencher apenas o mínimo inferível (ex. `device_types` default `["desktop", "mobile"]` como fallback documentado) e marcar `targeting_flag = 'missing_audience_data'` para excluir da análise de segmentação até ser corrigido na origem.

### 3.6.5 Inversão de Faixa Etária

**Tabela**: `marketing.adgroup` (`targeting_criteria.age_range`)

**Padrão**: `{"min_age": 65, "max_age": 18}`.

**Detecção**: `WHERE (targeting_criteria->'age_range'->>'min_age')::int > (targeting_criteria->'age_range'->>'max_age')::int`

**Correção**: `swap` determinístico (`LEAST`/`GREATEST` nos dois valores) — ao contrário de salário (RH), aqui não há ambiguidade de negócio: idade mínima e máxima trocadas é sempre um erro de digitação/envio, seguro de corrigir automaticamente.

### 3.6.6 Links Quebrados (Criativos)

**Tabela**: `marketing.ads` (`creative_data.final_url`, `creative_data.media_assets[].url`)

**Padrão**: URL sem `https://` (`"www.example.com/lp"`) ou com espaços (`"https://cdn.example.com/ad final.jpg"`).

**Detecção**: `WHERE final_url !~ '^https?://' OR final_url ~ '\s'`

**Correção**: normalizar (`trim` + prefixar `https://` quando plausível) e, se ainda inválida após normalização, marcar `creative_flag = 'broken_url'` — nunca deixar link quebrado seguir para relatórios de CTR por criativo sem sinalização.

### 3.6.7 Media Type Incompatível com Extensão

**Tabela**: `marketing.ads` (`creative_data.media_type` vs `creative_data.media_assets[].url`)

**Padrão**: `media_type = 'video'` mas a única URL em `media_assets` termina em `.jpg`/`.png`.

**Detecção**: `WHERE media_type = 'video' AND media_assets[0]->>'url' ~ '\.(jpg|jpeg|png)$'`

**Correção**: reclassificar `media_type` a partir da extensão real do arquivo (fonte de verdade é o asset, não o metadado da API) e sinalizar `creative_flag = 'media_type_mismatch'`.

### 3.6.8 Vazamento de Campo Entre Plataformas

**Tabela**: `marketing.daily_performance` (`metrics`, campo `quality_score`, exclusivo do Google Ads)

**Padrão**: anúncio de canal `meta_ads`/`tiktok_ads` com `metrics.quality_score` preenchido.

**Detecção**:
```sql
SELECT p.performance_id
FROM marketing.daily_performance p
JOIN marketing.ads a ON a.ad_id = p.ad_id
JOIN marketing.adgroup ag ON ag.ad_group_id = a.ad_group_id
JOIN marketing.campaign c ON c.campaign_id = ag.campaign_id
JOIN marketing.channel ch ON ch.channel_id = c.channel_id
WHERE ch.platform_name != 'google_ads'
  AND p.metrics->>'quality_score' IS NOT NULL;
```

**Correção**: anular o campo fora do domínio da plataforma (`quality_score = NULL` quando `platform_name != 'google_ads'`) antes de escrever na Wide Table de Silver — mapeamento de campo por plataforma deve ser explícito, não herdado do payload bruto.

### 3.6.9 Cliques > Impressões

**Tabela**: `marketing.daily_performance`

**Padrão**: `clicks > impressions` (comum por bots ou atraso de contabilização da plataforma).

**Detecção**: `WHERE clicks > impressions`

**Correção**: não zerar nem descartar — clampar `impressions` para `GREATEST(impressions, clicks)` em coluna derivada e sinalizar `metric_flag = 'clicks_exceed_impressions'` para auditoria, mantendo os valores originais intactos.

### 3.6.10 Custo Zero com Conversão

**Tabela**: `marketing.daily_performance`

**Padrão**: `cost = 0.00` e `conversions > 0` (ex. venda atribuída dias depois, dentro da janela de atribuição, sem custo lançado naquele dia).

**Detecção**: `WHERE cost = 0 AND conversions > 0`

**Correção**: não é erro a corrigir — é comportamento esperado de atribuição diferida. Apenas marcar `attribution_flag = 'delayed_conversion'` para não distorcer métricas de ROAS/CPA diário (o custo real já foi contabilizado em outro `date_ref`).

### 3.6.11 Métricas que Mudam Retroativamente

**Tabela**: `marketing.daily_performance`

**Padrão**: a mesma chave `(ad_id, date_ref)` recebe uma segunda carga, dias depois, com `conversions`/`conversion_value` diferentes (revisão da janela de atribuição do Google/Meta).

**Detecção**:
```sql
SELECT ad_id, date_ref, COUNT(*) as versions,
       COUNT(DISTINCT conversions) as distinct_conversion_values
FROM marketing.daily_performance
GROUP BY ad_id, date_ref
HAVING COUNT(*) > 1;
```

**Correção**: exige mudança na estratégia de carga, não só na limpeza — implementar upsert (`MERGE`/`ON CONFLICT (ad_id, date_ref) DO UPDATE`) mantendo a versão mais recente por `updated_at`, e opcionalmente uma tabela de auditoria (`daily_performance_revisions`) para rastrear o que mudou. Hoje o seeder não tem constraint única em `(ad_id, date_ref)`, então este é o mesmo gap estrutural de [3.5.5](#35-anomalias-em-recursos-humanos-1-3) (SCD2 quebrado), agora no domínio de fatos em vez de dimensão.

### 3.6.12 Funil de Vídeo Não-Monótono

**Tabela**: `marketing.daily_performance` (`metrics.video_views_25/50/75/100`)

**Padrão**: `video_views_75 > video_views_50` (fisicamente impossível — quem chegou a 75% também passou por 50%).

**Detecção**: `WHERE (metrics->>'video_views_100')::int > (metrics->>'video_views_75')::int OR (metrics->>'video_views_75')::int > (metrics->>'video_views_50')::int OR (metrics->>'video_views_50')::int > (metrics->>'video_views_25')::int`

**Correção**: clampar em cascata (`video_views_75 = LEAST(video_views_75, video_views_50)`, etc.) em colunas derivadas, preservando os valores brutos.

### 3.6.13 Reach > Impressions

**Tabela**: `marketing.daily_performance` (`metrics.reach`)

**Padrão**: `reach` (alcance único) maior que `impressions` (total, incluindo repetições) — fisicamente impossível.

**Detecção**: `WHERE (metrics->>'reach')::int > impressions`

**Correção**: mesmo padrão de 3.6.9/3.6.12 — clamp em coluna derivada + flag, nunca sobrescrever o dado bruto.

### 3.6.14 UTM Duplicado Entre Campanhas

**Tabela**: `marketing.campaign` (`utm_campaign`)

**Padrão**: dois `campaign_id` diferentes compartilhando o mesmo `utm_campaign` (erro humano comum: analista reaproveita a string de uma campanha antiga).

**Detecção**:
```sql
SELECT utm_campaign, COUNT(DISTINCT campaign_id) as campaigns
FROM marketing.campaign
GROUP BY utm_campaign
HAVING COUNT(DISTINCT campaign_id) > 1;
```

**Correção**: não é corrigível automaticamente (não dá pra saber qual campanha "é a certa" para aquele UTM) — sinalizar `attribution_flag = 'duplicate_utm'` e excluir da atribuição por UTM na Gold até resolução manual, usando `campaign_id` (chave técnica) como fallback de agrupamento.

---

## 3.7 Anomalias em Person — Customer, Email e Geografia (1-5%)

**Bug corrigido nesta seção**: `person_customer.py` sorteava `country_id` e `state_province_id` de duas listas **globais e independentes** (todos os países × todos os estados), sem nenhum join entre elas — ou seja, **100% dos clientes** podiam nascer com um estado que não pertence ao país sorteado, não como exceção controlada. O seeder agora monta um mapa `states_by_country` (via `StateProvinceModel.country_id`) e só sorteia países que já têm estado cadastrado, garantindo que o estado sempre pertença ao país por construção. A partir disso, a inconsistência geográfica (3.7.6) volta a ser uma exceção rara e deliberada, não o comportamento padrão.

### 3.7.1 Formatação de Nome (Case e Espaços)

**Tabela**: `person.customer` (`customer_name`, `last_name`)

**Padrão**: 5% dos nomes vêm em ALL CAPS (`"JOÃO DA SILVA"`), 5% em lowercase (`"joão da silva"`), ou com espaços extras nas pontas (`"  João da Silva  "`) — aplicado ao nome completo (`customer_name` + `last_name` juntos), não a um campo de cada vez.

**Detecção**: `WHERE customer_name != TRIM(customer_name) OR customer_name = UPPER(customer_name) OR customer_name = LOWER(customer_name)`

**Correção em Silver**: `TRIM()` + `INITCAP()` para normalizar, preservando o valor original em coluna de auditoria se o pipeline de Data Quality precisar rastrear a taxa de sujeira por período.

### 3.7.2 Datas de Nascimento Ilógicas

**Tabela**: `person.customer` (`birthdate`)

**Padrão**: nascido no futuro (ano 2030-2050) ou idade impossível (120-150 anos) — simulando erro de digitação na data.

**Detecção**: `WHERE birthdate > NOW() OR birthdate < NOW() - INTERVAL '110 years'`

**Correção**: não é corrigível automaticamente (não dá pra saber a data real) — marcar `birthdate_flag = 'invalid'` e excluir de métricas de idade/demografia até correção manual ou re-coleta.

### 3.7.3 CPF/CNPJ Formatado vs Não-Formatado

**Tabela**: `person.customer` (`social_number`)

**Padrão**: já implementado em `api/samples/customer.py` (50% com pontuação `123.456.789-00`, 50% só números `12345678900`).

**Correção**: normalizar via regex (remover tudo que não é dígito) antes de qualquer JOIN ou comparação entre tabelas que usem `social_number`.

### 3.7.4 Sintaxe Crítica de Email

**Tabela**: `person.customer_email` (`customer_email`)

**Padrão**: vírgula no lugar do ponto (`doug@gmail,com`), TLD ausente (`doug@gmail`) — além do que já existia (email sem `@`, espaço antes do `@`).

**Detecção**: `WHERE customer_email !~ '^[^@]+@[^@]+\.[a-zA-Z]{2,}$'`

**Correção**: regex de normalização + validação RFC 5322; se não for recuperável, marcar `email_flag = 'invalid'` e excluir de campanhas de marketing (não gerar bounce em provedor real).

### 3.7.5 Multi-valor Escondido no Mesmo Campo

**Tabela**: `person.customer_email` (`customer_email`)

**Padrão**: dois e-mails no mesmo registro, separados por `; ` (ex: `"doug@gmail.com; doug.work@empresa.com"`), em vez de duas linhas — erro clássico de formulário sem validação de campo único.

**Detecção**: `WHERE customer_email ~ '[;,]\s*\S+@\S+'`

**Correção**: split por `;`/`,` e materializar como registros adicionais (`is_primary=false`) na própria tabela, já que a modelagem correta é 1 linha por e-mail.

### 3.7.6 "Falso Estrangeiro" (Geografia Inconsistente)

**Tabela**: `person.customer` (`country_id`, `state_province_id`)

**Padrão**: `state_province_id` aponta para um estado que pertence a um país diferente do `country_id` do mesmo registro (ex: estado "São Paulo" com país "Estados Unidos").

**Detecção**:
```sql
SELECT c.customer_id
FROM person.customer c
JOIN person.stateprovince sp ON sp.state_province_id = c.state_province_id
WHERE sp.country_id != c.country_id;
```

**Correção**: não há como "adivinhar" qual dos dois campos está certo — sinalizar `geography_flag = 'country_state_mismatch'` para revisão manual. Nota de governança: como não existe *check constraint* no banco amarrando `state_province_id` ao `country_id` (JSONB e chaves soltas não suportam isso nativamente em `person.customer`), essa inconsistência **precisa** ser pega em Silver — o Postgres não impede sozinho.

### 3.7.7 Código Geográfico em Minúsculo

**Tabelas**: `person.countryregion` (`country_code`), `person.stateprovince` (`state_province_code`)

**Padrão**: 5% dos códigos vêm em minúsculo (`br` em vez de `BR`), simulando integração de API de terceiros que não normaliza antes de enviar.

**Detecção**: `WHERE country_code != UPPER(country_code)`

**Correção**: `UPPER()` na normalização — como são tabelas de domínio pequenas e estáveis, o ideal é corrigir na origem (constraint `CHECK (country_code = UPPER(country_code))`) além de normalizar em Silver.

---

## 3.8 Anomalias em Product — Catálogo, Categoria e Embeddings (1-5%)

**Colunas novas adicionadas nesta seção** (não existiam antes — sem elas, os cenários abaixo não eram sequer representáveis):
- `product.product.sell_start_date` / `sell_end_date` (`sell_end_date IS NULL` = produto ainda em venda)
- `product.productcategory.parent_category_id` (self-FK, `NULL` = categoria raiz)

**Gaps identificados que continuam fora de escopo** (documentados aqui para não serem confundidos com bugs):
- `product.productmodel` não é uma tabela de "modelo de produto" com nome livre — é um enum fixo de ~19 tipos (`SMARTPHONE`, `LAPTOP_COMPUTING`, etc.), sem campo de descrição rich-text. As anomalias de "tags HTML vazando" e "nomes quase-duplicados" não se aplicam a essa tabela; a de nome duplicado foi implementada em `product.product_name` (3.8.3), que é texto livre de verdade.
- `product.productcategory` não tem hierarquia real entre as 19 categorias (são todas irmãs) — `parent_category_id` foi adicionado como coluna, mas o seeder mantém todas como raiz (`NULL`) por padrão; só o chaos força um valor.

### 3.8.1 Margem Negativa / Preço Zerado

**Tabela**: `product.product` (`productdetails.price`, `productdetails.list_price`)

**Padrão**: `price` (venda) fica abaixo de `list_price` (custo de produção) ou zerado — simulando glitch de promoção mal configurada. Note que o nome dos campos é contraintuitivo: `list_price` = custo, `price` = venda.

**Detecção**: `WHERE (productdetails->>'price')::numeric < (productdetails->>'list_price')::numeric OR (productdetails->>'price')::numeric = 0`

**Correção**: nunca corrigir automaticamente (dado financeiro sensível) — sinalizar `margin_flag = 'negative_or_zero'` e alertar para revisão manual antes de qualquer relatório de lucro consumir a linha.

### 3.8.2 Peso com Unidade Suja (String em vez de Numeric)

**Tabela**: `product.product` (`productdetails.weight`)

**Padrão**: em vez de `1.5` (float limpo), o campo vem como `"1.5kg"`, `"1,5 kg"` ou `"1.5 KG"` — típico de integração com sistema legado que concatena a unidade no valor.

**Detecção**: `WHERE productdetails->>'weight' !~ '^[0-9]+(\.[0-9]+)?$'`

**Correção**: regex para extrair a parte numérica + normalizar separador decimal (`,` → `.`) antes de fazer `CAST` para `Numeric` na Silver.

### 3.8.3 Nome de Produto Quase-Duplicado

**Tabela**: `product.product` (`product_name`)

**Padrão**: dois produtos com o mesmo nome-base, mas formatação diferente (`"Smartphone Apple - Preto"` vs `"Smartphone-Apple---Preto"`) — erro clássico de cadastro duplo.

**Detecção**: `GROUP BY LOWER(REPLACE(product_name, '-', ' ')) HAVING COUNT(DISTINCT product_id) > 1`

**Correção**: normalizar (`lower` + colapsar separadores) antes de qualquer deduplicação; não apagar automaticamente — os dois `product_id` podem já ter vendas associadas.

### 3.8.4 Datas de Venda Ilógicas

**Tabela**: `product.product` (`sell_start_date`, `sell_end_date`)

**Padrão**: `sell_end_date` anterior ao `sell_start_date`.

**Detecção**: `WHERE sell_end_date IS NOT NULL AND sell_end_date < sell_start_date`

**Correção**: sinalizar `lifecycle_flag = 'invalid_sell_window'` — não é seguro inferir qual data está certa automaticamente.

### 3.8.5 Categoria Órfã (Parent Inexistente)

**Tabela**: `product.productcategory` (`parent_category_id`)

**Padrão**: `parent_category_id` aponta para um `category_id` que nunca existiu (simulando categoria pai deletada sem cascade).

**Detecção**:
```sql
SELECT c.category_id
FROM product.productcategory c
LEFT JOIN product.productcategory p ON p.category_id = c.parent_category_id
WHERE c.parent_category_id IS NOT NULL AND p.category_id IS NULL;
```

**Correção**: CTEs recursivas no Data Warehouse devem tratar `parent_category_id` órfão como raiz (`COALESCE` no join, não travar a query), além de alertar o time de catálogo.

### 3.8.6 Categoria Cíclica

**Tabela**: `product.productcategory` (`parent_category_id`)

**Padrão**: duas categorias apontando uma para a outra (`A.parent_category_id = B` e `B.parent_category_id = A`) — testa se a CTE recursiva do Data Warehouse tem proteção contra loop infinito.

**Detecção**: uma CTE recursiva sem `LIMIT`/profundidade máxima trava nesse caso — a query de detecção precisa ter guarda de profundidade (`WHERE depth < 20`) para não travar ela mesma.

**Correção**: adicionar `MAX RECURSION DEPTH` (ou equivalente) em toda CTE recursiva sobre `productcategory`, e alertar quando o limite é atingido (indica ciclo, não hierarquia legítima).

### 3.8.7 Vetor de Embedding Truncado (Dimension Mismatch)

**Tabela**: `product.productembedding` (`description_vector`)

**Padrão**: vetor com 1535 dimensões em vez de 1536 (falha silenciosa na etapa de inferência do modelo de ML).

**Nota importante**: diferente do que se poderia imaginar, isso **não é silencioso no Postgres** — a coluna é `vector(1536)` (dimensão fixa do pgvector), então o `INSERT` falha imediatamente com erro de dimensão. Vale simular mesmo assim para testar se o pipeline de carga trata esse erro de INSERT graciosamente (retry, quarentena, alerta) em vez de derrubar o job inteiro.

**Detecção**: falha no próprio `INSERT`; se for preciso auditar depois de já ter passado (ex: pipeline com validação fraca), `SELECT embedding_id FROM product.productembedding WHERE vector_dims(description_vector) != 1536`.

### 3.8.8 Valores NaN/Infinitos no Vetor

**Tabela**: `product.productembedding` (`description_vector`)

**Padrão**: 1 a 5 posições do vetor recebem `NaN`, `Infinity` ou `-Infinity` — simulando job de inferência que falhou parcialmente sem lançar exceção.

**Nota importante**: esta sim é a falha **verdadeiramente invisível** — pgvector aceita `NaN`/`Infinity` no INSERT sem reclamar, e o problema só aparece depois, quando o cálculo de cosine similarity retorna `NaN` para qualquer busca que toque esse vetor.

**Detecção**: `SELECT embedding_id FROM product.productembedding WHERE description_vector::text ~ 'NaN|Infinity'`

**Correção**: não há como reconstruir o vetor correto — remover da tabela de busca (ou marcar `embedding_flag = 'corrupted'` e excluir do índice) e reenfileirar o produto para reprocessamento do embedding.

---

## 3.9 Anomalias em Purchasing (1-5%)

**Bugs corrigidos nesta seção** (encontrados ao validar as regras, não eram só falta de chaos):
- Os 4 seeders de `purchasing` tinham imports quebrados (`api.models.purchasing.orderheader`/`orderdetail` não existem — os módulos reais são `purchaseorderheader.py`/`purchaseorderdetail.py`; `api.schemas.purchase_audit` não existe — o real é `api.schemas.purchasing.purchasing_orderaudit`; `product_id` era buscado em `ProductModel` (`productmodel.py`, tabela de tipos), não em `Product` (`product.py`, tabela real de produtos)). Nenhum dos 4 rodava antes desse fix.
- `purchase_purchaseorderdetail.py` comparava `h["status"] == 4` para decidir se um pedido estava recebido — mas `status` é salvo como string (`"completed"`, via `Status.value`), nunca `4`. Resultado: `received_qty`/`stocked_qty`/`is_fully_received` sempre viravam `0`/`0`/`False`, independente do status real. Corrigido para `h["status"] == Status.COMPLETED.value`.
- `purchase_purchaseorderaudit.py` não tinha função de carga (`async def seed_...`) nem `__main__` — não existia nenhum ponto de entrada pra rodar via Airflow/CLI, diferente dos outros 3 seeders do domínio. Adicionado.

### 3.9.1 Matemática Financeira que Não Bate

**Tabela**: `purchasing.purchaseorderheader`

**Padrão**: um "desconto manual" é aplicado direto em `total_due`, sem recalcular `sub_total`/`tax_amt`/`freight` — `sub_total + tax_amt + freight != total_due`.

**Detecção**: `WHERE total_due != (sub_total + tax_amt + freight)`

**Correção**: nunca sobrescrever `total_due` automaticamente (é dado financeiro aprovado por alguém) — sinalizar `accounting_flag = 'total_mismatch'` e alertar o financeiro para revisão manual da divergência.

### 3.9.2 Ship Date Antes de Order Date

**Tabela**: `purchasing.purchaseorderheader`

**Padrão**: `ship_date` anterior ao `order_date` — viagem no tempo logística.

**Detecção**: `WHERE ship_date < order_date`

**Correção**: sinalizar `logistics_flag = 'invalid_ship_window'`; não é seguro inferir qual data está certa.

### 3.9.3 Pedido Fantasma

**Tabela**: `purchasing.purchaseorderheader`

**Padrão**: `status = 'completed'` com todos os valores financeiros (`sub_total`, `tax_amt`, `freight`, `total_due`) zerados.

**Detecção**: `WHERE status = 'completed' AND total_due = 0.00`

**Correção**: bloquear faturamento de pedidos com valor zerado via regra de negócio na camada de serviço (não só detectar depois) — um pedido "completo" sem nenhum valor é, por definição, uma falha de processo.

### 3.9.4 Over-shipment (Física Impossível)

**Tabela**: `purchasing.purchaseorderdetail`

**Padrão**: `received_qty + rejected_qty > order_qty` — fornecedor enviou mais do que foi pedido e o almoxarifado deu entrada sem validar.

**Detecção**: `WHERE received_qty + rejected_qty > order_qty`

**Correção**: sinalizar `receiving_flag = 'overshipment'` para o time de recebimento; não truncar `received_qty` automaticamente (pode ser fraude ou erro de contagem, precisa de investigação).

### 3.9.5 Pedido Zumbi (Stale Order)

**Tabela**: `purchasing.purchaseorderdetail`

**Padrão**: `is_fully_received = false` com `due_date` de anos atrás (1000-1200 dias) — item nunca chegou e ninguém percebeu.

**Detecção**: `WHERE is_fully_received = false AND due_date < NOW() - INTERVAL '365 days'`

**Correção**: alimentar um relatório de "Pedidos Pendentes e Esquecidos" para o time de compras; não fechar a linha automaticamente.

### 3.9.6 Fornecedor "Caloteiro Ativo"

**Tabela**: `purchasing.vendor`

**Padrão**: `credit_rating` baixo (1-2) com `is_active = true`.

**Nota de implementação**: o schema `Vendor` já tem um `model_validator` que bloqueia exatamente essa combinação — o chaos muta o dict **depois** de passar pela validação (contornando o validator de propósito), simulando alguém forçando a ativação fora da API, direto no banco. Isso é o teste real de governança: como não existe *check constraint* equivalente no Postgres, essa combinação inválida só é pega se alguém auditar a tabela diretamente.

**Detecção**: `WHERE credit_rating <= 2 AND is_active = true`

**Correção**: alertar compliance imediatamente (não é um problema de "limpeza de dados", é uma falha de controle de acesso) e considerar reverter `is_active` automaticamente até revisão.

### 3.9.7 Colisão de Master Data (Mesmo CNPJ, Nomes Diferentes)

**Tabela**: `purchasing.vendor` (`vendor_name`, `vendor_details.tax_id`)

**Padrão**: dois ou três fornecedores com `vendor_name` diferente compartilhando o mesmo `tax_id`.

**Detecção**: `GROUP BY vendor_details->>'tax_id' HAVING COUNT(DISTINCT vendor_id) > 1`

**Correção**: Entity Resolution — não é seguro apagar um dos registros automaticamente (podem ter pedidos de compra vinculados a IDs diferentes); marcar como candidatos a merge e resolver manualmente, mantendo um `master_vendor_id` de referência.

### 3.9.8 Processo Pulado (Etapas de Auditoria Ausentes)

**Tabela**: `purchasing.purchaseorderaudit`

**Padrão**: a trilha de um pedido pula direto de `CREATED` para `CLOSED`, sem o `APPROVED` (ou `REJECTED`) intermediário obrigatório.

**Detecção**:
```sql
SELECT purchaseorderheader_id
FROM purchasing.purchaseorderaudit
GROUP BY purchaseorderheader_id
HAVING COUNT(*) < 2
   OR NOT bool_or(action IN ('approved', 'rejected'));
```

**Correção**: sinalizar `process_flag = 'skipped_approval'` e bloquear qualquer ação subsequente no pedido (ex: pagamento) até que a etapa de aprovação seja retroativamente registrada ou o pedido seja cancelado.

### 3.9.9 Aprovador Fantasma

**Tabela**: `purchasing.purchaseorderaudit`

**Padrão**: um evento `APPROVED`/`CLOSED` com `employee_id` que não existe em `humanresources.employee`, ou que existe mas não ocupa cargo de gerência/diretoria.

**Detecção**:
```sql
SELECT a.audit_id
FROM purchasing.purchaseorderaudit a
LEFT JOIN humanresources.employee e ON e.employee_id = a.employee_id
LEFT JOIN humanresources.jobrole j ON j.jobrole_id = e.jobrole_id
WHERE a.action IN ('approved', 'closed')
  AND (e.employee_id IS NULL OR j.job_title !~* 'gerente|manager|diretor|director|chief');
```

**Correção**: bloquear a aprovação na origem (validação na camada de serviço antes de gravar) — isso não deveria nem chegar ao banco; se já chegou, é indício de bypass da API (mesma classe de problema da 3.9.6).

---

## 3.10 Anomalias em Sales — Order, Eventlog, Return Order e Seller (1-5%)

**Bugs corrigidos nesta seção**:
- `sales_order.py` importava `ProductModel` de `api.models.product.productmodel` (tabela de tipos) em vez de `Product` de `api.models.product.product` para buscar `product_id` — mesma classe de bug já vista em purchasing.
- `sales_seller.py` importava `CountryRegionModel` de `api.models.person.countryregion` (sem underscore) — módulo inexistente; o real é `api.models.person.country_region`.
- `sales_eventlog.py` instanciava `SalesOrderEventLogSchema(event_id=None, ...)`, mas o schema exigia `event_id: int` obrigatório (`gt=0`) — todo evento gerado disparava `ValidationError`. Corrigido para `Optional[int] = Field(None, gt=0)`.

**Campos adicionados** (faltavam para as regras fazerem sentido):
- `sales.salesorder.order_status` agora varia (`open`/`finalized`/`partial_shipped`/`partial_payment`/`canceled`) em vez de ser sempre `"open"` fixo.
- `person.seller` (via `sales_seller.py`) agora popula `demographics.metrics.average_rating`, `total_gross_revenue` e `chargeback_rate`, que não existiam na geração limpa.

### 3.10.1 Matemática Quebrada (Desconto Fantasma)

**Tabela**: `sales.salesorder` (`total_amount` vs soma de `order_items`)

**Padrão**: `total_amount` fica bem abaixo da soma de `unit_price * quantity` dos itens — simulando desconto aplicado direto no cabeçalho sem ratear entre as linhas.

**Detecção**: comparar `total_amount` contra `SUM((item->>'unit_price')::numeric * (item->>'quantity')::int)` extraído de `order_items`.

**Correção**: nunca recalcular `total_amount` automaticamente (pode ter sido um desconto legítimo) — sinalizar `financial_flag = 'total_mismatch'` e alertar o financeiro para investigar a origem do desconto.

### 3.10.2 Pagamento Esquizofrênico

**Tabela**: `sales.salesorder` (`payment_info`)

**Padrão**: `is_split = false` mas `transactions` tem 3 pagamentos diferentes.

**Detecção**: `WHERE (payment_info->>'is_split')::boolean = false AND jsonb_array_length(payment_info->'transactions') > 1`

**Correção**: corrigir `is_split` para `true` na Silver quando `transactions` tiver mais de 1 item (é um campo derivável, não uma fonte de verdade independente).

### 3.10.3 Pedido de Schrödinger

**Tabela**: `sales.salesorder` (`order_status` vs `payment_info.transactions[].status`)

**Padrão**: `order_status = 'finalized'` mas a transação de pagamento está `unpaid` ou `refunded`.

**Detecção**: `WHERE order_status = 'finalized' AND payment_info->'transactions'->0->>'status' IN ('unpaid', 'refunded')`

**Correção**: bloquear esse pedido do cálculo de Receita Bruta até resolução — um pedido "finalizado" sem pagamento confirmado é contabilmente inválido, nunca inferir receita a partir de `order_status` isoladamente.

### 3.10.4 Viagem no Tempo Logística

**Tabela**: `sales.ordereventlog` (`event_date`)

**Padrão**: um evento posterior na sequência lógica (ex: `delivered`) chega com `event_date` anterior a um evento anterior do mesmo pedido (ex: `item_picked`) — coletor do armazém sem internet que descarregou os logs com atraso.

**Detecção**:
```sql
SELECT e1.salesorder_id, e1.event_type, e1.event_date, e2.event_type, e2.event_date
FROM sales.ordereventlog e1
JOIN sales.ordereventlog e2 ON e1.salesorder_id = e2.salesorder_id AND e1.event_id < e2.event_id
WHERE e1.event_date > e2.event_date;
```

**Correção**: a camada Silver deve ordenar eventos por `event_date` (tempo real do acontecimento), não pela ordem de chegada/`event_id` — reconstituir a timeline correta antes de qualquer cálculo de SLA de entrega.

### 3.10.5 Teletransporte GPS

**Tabela**: `sales.ordereventlog` (`event_data.gps_location`)

**Padrão**: coordenadas em `(0.0, 0.0)` ("Null Island", no Golfo da Guiné) ou fisicamente impossíveis (`lat: 999.9`).

**Detecção**: `WHERE (event_data->'gps_location'->>'lat')::float NOT BETWEEN -90 AND 90 OR (event_data->'gps_location'->>'long')::float NOT BETWEEN -180 AND 180 OR (event_data->'gps_location'->>'lat' = '0' AND event_data->'gps_location'->>'long' = '0')`

**Correção**: descartar a coordenada (não usar em cálculos de rota/distância) e marcar `gps_flag = 'invalid'`; não é seguro interpolar uma posição a partir de eventos vizinhos sem contexto adicional.

### 3.10.6 Reembolso Infinito

**Tabela**: `sales.returnorder` (`refund_amount`)

**Padrão**: `refund_amount` 10x maior que o valor esperado (`unit_price * quantity` do item original) — atendente digitou um zero a mais no estorno manual.

**Detecção**: comparar `refund_amount` contra o valor do item na `order_items` do pedido original; alertar se a razão for > 2x.

**Correção**: nunca ajustar o valor automaticamente (é uma transação financeira já processada) — sinalizar `refund_flag = 'amount_anomaly'` para auditoria e possível estorno do estorno.

### 3.10.7 Inspeção Fantasma

**Tabela**: `sales.returnorder` (`inspection_data`)

**Padrão**: `return_status = 'refunded'` e `is_restockable = true`, mas `inspector_user_id` e `technical_notes` (e `inspection_date`, que já é omitido por padrão no seeder) estão nulos — alguém liberou o estorno e a reintegração ao estoque sem examinar o produto.

**Detecção**: `WHERE return_status = 'refunded' AND (inspection_data->>'is_restockable')::boolean = true AND inspection_data->>'inspector_user_id' IS NULL`

**Correção**: bloquear `is_restockable = true` sem `inspector_user_id` preenchido na camada de serviço (regra de negócio, não só detecção); em dados já gravados, reverter `is_restockable` para `false` até inspeção retroativa.

### 3.10.8 Parceiro Tóxico (Seller de Alto Risco)

**Tabela**: `person.seller` (`demographics.metrics`, `is_active`)

**Padrão**: `chargeback_rate` entre 0.5 e 0.9 e `average_rating` entre 1.0 e 1.5, mas `is_active = true`.

**Detecção**: `WHERE (demographics->'metrics'->>'chargeback_rate')::numeric > 0.3 AND (demographics->'metrics'->>'average_rating')::numeric < 2.0 AND is_active = true`

**Correção**: view de "Sellers de Alto Risco Operando" para o time de risco monitorar continuamente — não desativar automaticamente (pode haver contrato/negociação em andamento), mas alertar diariamente enquanto a combinação persistir.

### 3.10.9 Agência e Conta Bancária Sujas

**Tabela**: `person.seller` (`demographics.bank_account`)

**Padrão**: `account_number` com caractere não-numérico (`"12345-X"`) ou `agency` vazia (`""`).

**Detecção**: `WHERE demographics->'bank_account'->>'account_number' !~ '^[0-9-]+$' OR demographics->'bank_account'->>'agency' = ''`

**Correção**: normalizar/validar antes de qualquer tentativa de payout — nunca deixar o time de pagamentos tentar transferir para uma conta com formato inválido; bloquear o repasse e notificar o seller para corrigir o cadastro.

---

## 3.11 Anomalias em Warehouse — Transaction History, Inventory e Location (1-5%)

**Bugs corrigidos nesta seção** (code review feito pelo usuário antes do chaos, achados adicionais durante a implementação):
- `TransactionHistorySchema.validate_transaction_logic` bloqueava `quantity < 0` com `raise ValueError`, contradizendo a própria descrição do campo ("Positivo para Entrada, Negativo para Saída") — toda saída de estoque (venda, ajuste negativo) era rejeitada pelo Pydantic. Removida a checagem de sinal, mantida apenas a checagem de `quantity == 0`.
- `LocationCapabilities.warehouse_type: str = WarehouseType` atribuía a **classe** do Enum como valor padrão (não um membro dela) a um campo tipado como `str`. Corrigido para `warehouse_type: WarehouseType = Field(default=WarehouseType.DRY)`.
- `transaction_history.py` importava `ProductModel` de `api.models.product.product` (a classe real é `Product`) e `TransactionTypeModel` de `api.models.warehouse.transactiontype` (a classe real é `WarehouseTransactionType`) — dois `ImportError` garantidos.
- `product_inventory.py` usava `random.randint`/`random.choice` sem nunca importar `random` (`NameError` garantido), **e** importava `ProductModel` em vez de `Product` (mesmo bug de `transaction_history.py`).
- `ProductInventorySchema.inventory_id` exigia `int` obrigatório, mas o seeder passa `inventory_id=None` (autoincrement) — mesma classe de bug já corrigida em `SalesOrderEventLogSchema.event_id` (seção 3.10). Corrigido para `Optional[int]`.

### 3.11.1 Quebra de Cadeia de Frio (IoT Glitch)

**Tabela**: `warehouse.transactionhistory` (`traceability_info.iot_sensor_data.temp_at_scan`)

**Padrão**: uma transação envolvendo uma localização `cold_storage` registra `temp_at_scan = 45.0`.

**Detecção**:
```sql
SELECT t.transaction_id
FROM warehouse.transactionhistory t
JOIN warehouse.warehouselocation wl
  ON wl.warehouselocation_id IN (
       (t.traceability_info->>'source_warehouselocation_id')::int,
       (t.traceability_info->>'target_warehouselocation_id')::int
     )
WHERE wl.capabilities->>'warehouse_type' = 'cold_storage'
  AND (t.traceability_info->'iot_sensor_data'->>'temp_at_scan')::float > 8.0;
```

**Correção**: pipeline de Compliance dedicado que dispara alerta "Lote Comprometido por Quebra de Cadeia de Frio" em tempo real (não em batch noturno) — produto potencialmente impróprio para consumo/uso não pode esperar o próximo run do Data Lake.

### 3.11.2 Buraco Negro Logístico

**Tabela**: `warehouse.transactionhistory` (`traceability_info.target_warehouselocation_id`)

**Padrão**: uma transferência (tipo com `action_effect = 'neutral'`) tem `source_warehouselocation_id` preenchido, mas `target_warehouselocation_id` nulo — a carga saiu de um armazém e nunca chegou a lugar nenhum registrado.

**Detecção**:
```sql
SELECT t.transaction_id
FROM warehouse.transactionhistory t
JOIN warehouse.transactiontype tt ON tt.transactiontype_id = t.transactiontype_id
WHERE tt.action_effect = 'neutral'
  AND t.traceability_info->>'source_warehouselocation_id' IS NOT NULL
  AND t.traceability_info->>'target_warehouselocation_id' IS NULL;
```

**Correção**: alertar operações/segurança patrimonial imediatamente (pode ser roubo de carga, não só falha de sistema) — nunca inferir um destino automaticamente.

### 3.11.3 Zumbi Perecível

**Tabela**: `warehouse.productinventory` (`quantity`, `is_active`, `attributes.expiration_date`)

**Padrão**: `quantity` alto (3000-8000), `is_active = true`, mas `expiration_date` vencida há 700-900 dias.

**Detecção**: `WHERE is_active = true AND quantity > 0 AND (attributes->>'expiration_date')::date < CURRENT_DATE - INTERVAL '30 days'`

**Correção**: rotina de write-off automático não é segura (pode haver erro de cadastro de validade) — sinalizar `inventory_flag = 'expired_active_stock'` e excluir da valoração financeira de estoque (relatório de custo/CEO) até confirmação física, mas sem apagar o registro.

### 3.11.4 Lote Quântico

**Tabela**: `warehouse.productinventory` (`attributes.lot_number`, `attributes.manufacturing_date`)

**Padrão**: produto com `quantity > 0` mas `lot_number` e `manufacturing_date` nulos — quebra de rastreabilidade em itens que normalmente exigem (alimentos, medicamentos, eletrônicos com garantia).

**Detecção**: `WHERE quantity > 0 AND attributes->>'lot_number' IS NULL AND attributes->>'manufacturing_date' IS NULL`

**Correção**: bloquear a venda/expedição desse item até o lote ser identificado retroativamente — não é seguro despachar produto sem rastreabilidade em categorias regulamentadas.

### 3.11.5 Polo Norte no Deserto

**Tabela**: `warehouse.warehouselocation` (`capabilities.warehouse_type` vs `capabilities.temperature_range`)

**Padrão**: `warehouse_type = 'cold_storage'` mas `temperature_range = {min_temp: 25.0, max_temp: 40.0}` (faixa de câmara quente).

**Detecção**: `WHERE capabilities->>'warehouse_type' = 'cold_storage' AND (capabilities->'temperature_range'->>'min_temp')::float > 0`

**Correção**: cadastro inconsistente bloqueia operação — alertar o time de facilities/cadastro; a camada Silver deve marcar `location_flag = 'temperature_mismatch'` e impedir que produtos "cold chain" sejam roteados para essa localização até correção manual.

### 3.11.6 Teletransportação Geográfica

**Tabela**: `warehouse.warehouselocation` (`geo_location.postal_code` vs `geo_location.country_name`)

**Padrão**: CEP em formato de outro país (ex: `"90210"` americano) num registro cujo país é diferente.

**Detecção**: validar `postal_code` contra o formato esperado do `country_name`/`country_id` (regex por país); alertar divergência.

**Correção**: não é seguro plotar essa localização no mapa de BI sem correção — excluir de visualizações geográficas até o CEP ser validado/corrigido, sinalizando `geo_flag = 'postal_code_country_mismatch'`.

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

### 6.2 Dashboard de Qualidade — Recursos Humanos

```sql
-- Salário fora da faixa do cargo (join com jobrole atual)
SELECT
    'Salary Out of Range' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j
  ON e.job_role_id = j.jobrole_id AND j.is_current = true
WHERE e.current_salary NOT BETWEEN j.min_salary AND j.max_salary;

-- Vendedores sem comissão cadastrada
SELECT
    'Sales Without Commission' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j ON e.job_role_id = j.jobrole_id
WHERE j.job_title ILIKE '%sales%'
  AND COALESCE((e.role_attributes->>'comission_pct')::numeric, 0) = 0;

-- Vendedores sem território/região
SELECT
    'Sales Without Territory' as issue_type,
    COUNT(*) as issue_count
FROM humanresources.employee e
JOIN humanresources.jobrole j ON e.job_role_id = j.jobrole_id
WHERE j.job_title ILIKE '%sales%'
  AND (e.role_attributes->'territory_codes' IS NULL
       OR jsonb_array_length(e.role_attributes->'territory_codes') = 0);

-- Registros SCD2 quebrados (mais de um "atual" por funcionário)
SELECT employee_id, COUNT(*) as current_versions
FROM humanresources.employee
WHERE is_current = true
GROUP BY employee_id
HAVING COUNT(*) > 1;
```

### 6.3 Dashboard de Detecção de Fraudes

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
   - ✅ Feito para `marketing`: as funções ficam em `dbapi/api/seeder/chaos/marketing_chaos.py`, reexportadas por `dbapi/api/seeder/chaos/__init__.py` e importadas pelos 4 seeders (`from api.seeder.chaos import apply_campaign_chaos, ...`). Usar essa pasta como padrão para os demais domínios (`dbapi/api/seeder/chaos/<dominio>_chaos.py`).
   - ✅ Feito para `person`: `dbapi/api/seeder/chaos/person_chaos.py` (`apply_customer_chaos`, `apply_customer_email_chaos`, `apply_geography_code_chaos`, `apply_false_foreigner_chaos`), importadas por `person_customer.py`, `person_customer_email.py`, `person_state_province.py` e `person_country.py`.
   - ✅ Feito para `product`: `dbapi/api/seeder/chaos/product_chaos.py` (`apply_product_chaos`, `apply_category_chaos`, `apply_embedding_chaos`), importadas por `product.py`, `product_category.py` e `product_embedding.py`. Adicionou as colunas novas `product.sell_start_date`/`sell_end_date` e `productcategory.parent_category_id` (ver seção 3.8).
   - ✅ Feito para `purchasing`: `dbapi/api/seeder/chaos/purchasing_chaos.py` (`apply_purchaseorderheader_chaos`, `apply_purchaseorderdetail_chaos`, `apply_vendor_chaos`, `apply_purchase_audit_chaos`), importadas pelos 4 seeders do domínio. Corrigiu imports quebrados que impediam qualquer um dos 4 seeders de rodar, o bug do `status == 4`, e adicionou ponto de entrada (`seed_purchase_order_audit`) que não existia em `purchase_purchaseorderaudit.py` (ver seção 3.9).
   - ✅ Feito para `sales`: `dbapi/api/seeder/chaos/sales_chaos.py` (`apply_sales_order_chaos`, `apply_eventlog_chaos`, `apply_return_order_chaos`, `apply_seller_chaos`), importadas pelos 4 seeders do domínio. Corrigiu imports quebrados (`ProductModel`→`Product`, `countryregion`→`country_region`) e um `ValidationError` garantido em `sales_eventlog.py` (`event_id` obrigatório recebendo `None`). Nota: `apply_eventlog_chaos` opera sobre instâncias já construídas de `SalesOrderEventLogSchema`, não sobre `dict` — é o único chaos do projeto que muta atributos de objetos Pydantic diretamente, em vez de dicts pós-`model_dump()` (ver seção 3.10).
   - ✅ Feito para `warehouse`: `dbapi/api/seeder/chaos/warehouse_chaos.py` (`apply_transaction_history_chaos`, `apply_inventory_chaos`, `apply_warehouse_location_chaos`), importadas pelos 3 seeders do domínio. Corrigiu 2 bugs de schema Pydantic encontrados via code review do usuário (`TransactionHistorySchema` bloqueava `quantity` negativo contradizendo sua própria descrição; `LocationCapabilities.warehouse_type` atribuía a classe do Enum em vez de um membro dela), além de imports quebrados (`ProductModel`→`Product`, `TransactionTypeModel`→`WarehouseTransactionType`), `product_inventory.py` sem `import random`, e `ProductInventorySchema.inventory_id` com o mesmo bug de `event_id`/`gt=0` obrigatório recebendo `None` (ver seção 3.11).

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

7. **Resolver a causa raiz do histórico em `humanresources.employee`**:
   - Hoje o seeder sempre grava `is_current=true` sem fechar versões anteriores (ver [3.5.5](#355-scd2-quebrado--múltiplos-atuais-ou-buracos-temporais))
   - Implementar upsert/MERGE por `employee_id` no processo de carga, comparando atributos rastreados (salário, cargo, departamento) com a versão `is_current=true` existente
   - Só assim a Camada Silver poderá, no futuro, corrigir anomalias por histórico real do indivíduo em vez de depender de dados de referência/estatística de pares

8. **Arquitetura futura: Silver Corrections + Quarentena + Great Expectations** (planejado, ainda não implementado — registrado aqui como ponto de partida):
   - **Onde roda**: Spark, no mesmo estilo dos jobs já existentes em `spark/jobs/silver_layer_advworks_scripts/`. Nada de serviço novo — o Great Expectations entra como uma etapa a mais dentro do próprio job de Silver, usando a Spark execution engine do GX.
   - **Pipeline por domínio**: (1) lê o Bronze; (2) roda uma suite de Expectations — cada regra já documentada nas seções 3.5 a 3.11 deste arquivo (com a query de "Detecção" pronta) vira uma `Expectation`; (3) linhas que passam seguem para a transformação de Silver normal; (4) linhas que falham vão para uma **tabela de quarentena** (mesma estrutura + colunas de auditoria `dq_rule_id`, `dq_failure_reason`, `quarantined_at`), vivendo no mesmo lakehouse Iceberg do Silver (ex: `iceberglake.quarantine.<tabela>`), não no Postgres — assim o Trino já enxerga sem precisar de outra fonte.
   - **Job de correção**: um registro de funções (mesmo espírito do `dbapi/api/seeder/chaos/`, mas de correção em vez de injeção) — uma função por regra, cada uma com uma política `auto_fix` (normalizações objetivamente seguras: trim, uppercase, cast, clamp) ou `flag_only` (dado financeiro sensível — nunca corrigir sozinho, só sinalizar para revisão humana, conforme a política que já está descrita em cada seção 3.x deste documento). Lê da quarentena, aplica a correção quando `auto_fix=True` e faz **MERGE** (nunca append) de volta para o Silver, "promovendo" a linha.
   - **Late arriving / backfill dinâmico**: a chave do MERGE é sempre a chave natural da tabela (ex: `(ad_id, date_ref)` em marketing, não um ID de carga), e o job roda sobre uma **janela móvel configurável** (ex: últimos 7-15 dias), nunca só "ontem". Isso faz o mesmo job de rotina também funcionar como backfill automático quando uma correção/dado atrasado chega — sem precisar de um "modo backfill" à parte.
   - **Escopo inicial acordado**: prototipar essa cadeia completa (leitura → GX → quarentena → correção → MERGE) de ponta a ponta em **um domínio primeiro** (candidato: `marketing`, por já ter as queries de detecção mais detalhadas nas seções 3.6) antes de generalizar para os outros 5 domínios.
   - **Em aberto, não bloqueia o início**: se o caminho pós-Silver será Gold → Trino → Postgres (para o Power BI) ou Power BI conectando direto na Gold. Essa decisão é independente do trabalho de Bronze→Silver+quarentena acima.

---

## Conclusão

Este documento serve como **fonte única de verdade** para todos os tipos de anomalias injetadas nos dados. Deve ser mantido atualizado sempre que novos tipos de chaos forem adicionados aos seeders.

**Importante**: Esses dados "sujos" são **intencionais e documentados**, não são bugs! Eles existem para treinar pipelines de limpeza e detecção de fraudes em um ambiente controlado.
