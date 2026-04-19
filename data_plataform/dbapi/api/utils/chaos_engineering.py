"""
Chaos Engineering Utilities for Data Quality Testing

This module provides functions to inject controlled anomalies into seeded data
to simulate real-world data quality issues, fraud patterns, and operational failures.

These anomalies are intentional and documented, designed for:
- Testing data cleaning pipelines
- Training fraud detection models
- Validating data quality rules
- Preparing analysts for production scenarios
"""

import uuid
import random
from typing import List, Dict, Any, Callable
from datetime import datetime, timedelta
from decimal import Decimal


# ==================== CHAOS STRATEGIES ====================
# Cada função recebe um registro e retorna None ou um novo registro (para duplicações)

def chaos_duplicate(record: Dict[str, Any]) -> Dict[str, Any] | None:
    """Duplica registro com variação de case, espaços ou acentos."""
    if "customer_name" not in record:
        return None

    duplicated = record.copy()
    variation = random.choice([
        lambda s: s.upper(),
        lambda s: s.lower(),
        lambda s: f"  {s}  ",
        lambda s: s.replace("ã", "a").replace("ç", "c").replace("á", "a").replace("é", "e")
    ])
    duplicated["customer_name"] = variation(duplicated["customer_name"])
    duplicated["row_guid"] = uuid.uuid4()
    return duplicated


def chaos_dirty_cpf(record: Dict[str, Any]) -> None:
    """Injeta CPF inválido."""
    if "cpf" in record:
        record["cpf"] = random.choice([
            "000.000.000-00", "111.111.111-11", "123.456.789-00",
            "12345678900", "999.999.999-99"
        ])


def chaos_dirty_phone(record: Dict[str, Any]) -> None:
    """Injeta telefone inválido."""
    if "phone" in record:
        record["phone"] = random.choice([
            "0000000", "11111111111", "(00) 0000-0000",
            "99 99999-9999", "+55 11 91234-567"
        ])


def chaos_dirty_email(record: Dict[str, Any]) -> None:
    """Injeta email inválido."""
    if "email" in record:
        record["email"] = random.choice([
            "email@dominio", "@dominio.com", "email@@dominio.com",
            "email@dominio .com", "test@test.test"
        ])


def chaos_dirty_cep(record: Dict[str, Any]) -> None:
    """Injeta CEP inválido."""
    if "cep" in record:
        record["cep"] = random.choice([
            "00000-000", "XXXXX-XXX", "12345-678", "01310100"
        ])


def chaos_dirty_name(record: Dict[str, Any]) -> None:
    """Injeta nome inválido."""
    if "customer_name" in record:
        record["customer_name"] = random.choice([
            "João123", "Maria###", "Cliente Teste", "XXXX", "   ", "A"
        ])


def chaos_outlier_value(record: Dict[str, Any]) -> None:
    """Multiplica valor por 5-10x (outlier legítimo)."""
    if "total_amount" in record:
        multiplier = random.randint(5, 10)
        if isinstance(record["total_amount"], Decimal):
            record["total_amount"] = record["total_amount"] * multiplier
        else:
            record["total_amount"] = float(record["total_amount"]) * multiplier


def chaos_outlier_quantity(record: Dict[str, Any]) -> None:
    """Injeta quantidade anormalmente alta."""
    if "quantity" in record:
        record["quantity"] = random.randint(1000, 10000)


def chaos_outlier_lead_time(record: Dict[str, Any]) -> None:
    """Injeta lead time muito longo (90-180 dias)."""
    if "ship_date" in record and "order_date" in record:
        record["ship_date"] = record["order_date"] + timedelta(days=random.randint(90, 180))


def chaos_fraud_fake_cpf(record: Dict[str, Any]) -> None:
    """CPF falso com nome realista (fraude)."""
    if "cpf" in record:
        record["cpf"] = "000.000.000-00"


def chaos_fraud_fake_address(record: Dict[str, Any]) -> None:
    """Nome falso com endereço real (fraude)."""
    if "customer_name" in record:
        record["customer_name"] = random.choice([
            "Cliente Teste", "Fulano de Tal", "João Silva", "Maria Santos"
        ])


def chaos_fraud_cloned_card(record: Dict[str, Any]) -> None:
    """Compras geograficamente impossíveis (cartão clonado)."""
    if "shipping_city" in record:
        record["shipping_city"] = "São Paulo"
        if "billing_city" in record:
            record["billing_city"] = "Rio de Janeiro"
        if "order_date" in record:
            record["order_date"] = datetime.now()


def chaos_fraud_high_value_suspicious(record: Dict[str, Any]) -> None:
    """Alto valor com dados suspeitos (email gratuito + cadastro novo)."""
    if "total_amount" in record:
        record["total_amount"] = Decimal(random.uniform(50000.0, 100000.0)).quantize(Decimal("0.01"))
    if "email" in record:
        record["email"] = f"teste{random.randint(1, 999)}@gmail.com"


def chaos_fraud_multiple_cards_same_address(record: Dict[str, Any]) -> None:
    """Múltiplos cartões no mesmo endereço (padrão de fraude)."""
    # Implementação depende de lógica cross-record (não aplicável aqui, apenas marcador)
    pass


def chaos_fraud_duplicate_order(record: Dict[str, Any]) -> Dict[str, Any] | None:
    """Duplica ordem de compra (possível suborno ou erro)."""
    if "purchaseorderheader_id" not in record:
        return None

    duplicated = record.copy()
    duplicated["purchaseorderheader_id"] = f"PO-{random.randint(100000000, 999999999)}"
    if "order_date" in duplicated:
        duplicated["order_date"] = record["order_date"] + timedelta(days=1)
    duplicated["row_guid"] = uuid.uuid4()
    return duplicated


def chaos_fraud_phantom_vendor(record: Dict[str, Any]) -> None:
    """Fornecedor fantasma (CNPJ inválido)."""
    if "vendor_name" in record:
        record["vendor_name"] = f"Fornecedor Fantasma {random.randint(1, 999)}"
    if "cnpj" in record:
        record["cnpj"] = "00.000.000/0000-00"


def chaos_fraud_overpriced(record: Dict[str, Any]) -> None:
    """Superfaturamento (preço 3-8x acima do normal)."""
    if "unit_price" in record:
        multiplier = random.randint(3, 8)
        if isinstance(record["unit_price"], Decimal):
            record["unit_price"] = record["unit_price"] * multiplier
        else:
            record["unit_price"] = float(record["unit_price"]) * multiplier


def chaos_fraud_unauthorized_approval(record: Dict[str, Any]) -> None:
    """Aprovação sem autorização (marcador, validação na API)."""
    # Validação deve ser feita contra tabela de permissões
    pass


def chaos_fraud_personal_delivery(record: Dict[str, Any]) -> None:
    """Entrega em endereço pessoal ao invés de warehouse (marcador)."""
    # Validação deve verificar se endereço está em warehouselocation
    pass


def chaos_incomplete_payment(record: Dict[str, Any]) -> None:
    """Pagamento pendente há muito tempo (> 30 dias)."""
    if "order_status" in record:
        record["order_status"] = "pending"
    if "order_date" in record:
        record["order_date"] = datetime.now() - timedelta(days=random.randint(30, 90))


def chaos_incomplete_shipping(record: Dict[str, Any]) -> None:
    """Pago mas não enviado (> 15 dias)."""
    if "payment_info" in record and "transactions" in record["payment_info"]:
        record["payment_info"]["transactions"][0]["status"] = "paid"
    if "order_date" in record:
        record["order_date"] = datetime.now() - timedelta(days=random.randint(15, 30))


def chaos_incomplete_delivery(record: Dict[str, Any]) -> None:
    """Enviado mas não entregue (> 90 dias)."""
    if "shipping_status" in record:
        record["shipping_status"] = "in_transit"
    if "shipping_date" in record:
        record["shipping_date"] = datetime.now() - timedelta(days=random.randint(90, 180))


def chaos_incomplete_product_missing(record: Dict[str, Any]) -> None:
    """Produto inexistente (ID improvável)."""
    if "product_id" in record:
        record["product_id"] = 99999


def chaos_warehouse_lost_transfer(record: Dict[str, Any]) -> None:
    """Transferência perdida (em trânsito há > 90 dias)."""
    if "transaction_date" in record:
        record["transaction_date"] = datetime.now() - timedelta(days=random.randint(90, 180))
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        record["traceability_info"]["status"] = "in_transit"


def chaos_warehouse_quantity_mismatch(record: Dict[str, Any]) -> None:
    """Divergência de quantidade (recebido < enviado)."""
    if "quantity" in record:
        reduction = random.uniform(0.05, 0.15)
        record["quantity"] = int(record["quantity"] * (1 - reduction))


def chaos_warehouse_negative_stock(record: Dict[str, Any]) -> None:
    """Estoque negativo (overselling)."""
    if "quantity" in record:
        record["quantity"] = random.randint(-50, -1)


def chaos_warehouse_missing_location(record: Dict[str, Any]) -> None:
    """Produto sem localização física."""
    if "warehouselocation_id" in record:
        record["warehouselocation_id"] = None


def chaos_warehouse_quality_fail(record: Dict[str, Any]) -> None:
    """Produto reprovado mas enviado."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        record["traceability_info"]["quality_check_status"] = "fail"


def chaos_warehouse_cold_chain_break(record: Dict[str, Any]) -> None:
    """Cold chain quebrada (temperatura fora do range)."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        if "iot_sensor_data" in record["traceability_info"]:
            if isinstance(record["traceability_info"]["iot_sensor_data"], dict):
                record["traceability_info"]["iot_sensor_data"]["temp_at_scan"] = random.uniform(15.0, 25.0)


def chaos_warehouse_same_location_transfer(record: Dict[str, Any]) -> None:
    """Transferência para o mesmo local (erro de sistema)."""
    if "traceability_info" in record and isinstance(record["traceability_info"], dict):
        if "source_warehouselocation_id" in record["traceability_info"]:
            record["traceability_info"]["target_warehouselocation_id"] = record["traceability_info"]["source_warehouselocation_id"]


def chaos_warehouse_missing_traceability(record: Dict[str, Any]) -> None:
    """Transação sem rastreabilidade."""
    if "traceability_info" in record:
        record["traceability_info"] = None


def chaos_negative_cost(record: Dict[str, Any]) -> None:
    """Custo negativo (erro de sistema)."""
    if "actual_cost" in record:
        if isinstance(record["actual_cost"], Decimal):
            record["actual_cost"] = -abs(record["actual_cost"])
        else:
            record["actual_cost"] = -abs(float(record["actual_cost"]))


def chaos_negative_quantity(record: Dict[str, Any]) -> None:
    """Quantidade negativa (bug de integração)."""
    if "quantity" in record:
        record["quantity"] = -abs(record["quantity"])


def chaos_negative_freight(record: Dict[str, Any]) -> None:
    """Frete negativo (ajuste mal implementado)."""
    if "freight" in record:
        if isinstance(record["freight"], Decimal):
            record["freight"] = -abs(record["freight"])
        else:
            record["freight"] = -abs(float(record["freight"]))


def chaos_missing_order_ref(record: Dict[str, Any]) -> None:
    """Movimentação sem ordem de origem."""
    if "reference_order_id" in record:
        record["reference_order_id"] = None


def chaos_missing_vendor_ref(record: Dict[str, Any]) -> None:
    """Compra sem fornecedor (erro de sistema)."""
    if "vendor_id" in record:
        record["vendor_id"] = None


# ==================== CHAOS STRATEGY MAP ====================
CHAOS_STRATEGIES: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any] | None]] = {
    # Duplicidade
    "duplicate": chaos_duplicate,

    # Dados sujos
    "dirty_cpf": chaos_dirty_cpf,
    "dirty_phone": chaos_dirty_phone,
    "dirty_email": chaos_dirty_email,
    "dirty_cep": chaos_dirty_cep,
    "dirty_name": chaos_dirty_name,

    # Outliers
    "outlier_value": chaos_outlier_value,
    "outlier_quantity": chaos_outlier_quantity,
    "outlier_lead_time": chaos_outlier_lead_time,

    # Fraudes em compras
    "fraud_fake_cpf": chaos_fraud_fake_cpf,
    "fraud_fake_address": chaos_fraud_fake_address,
    "fraud_cloned_card": chaos_fraud_cloned_card,
    "fraud_high_value_suspicious": chaos_fraud_high_value_suspicious,
    "fraud_multiple_cards_same_address": chaos_fraud_multiple_cards_same_address,

    # Fraudes em purchasing
    "fraud_duplicate_order": chaos_fraud_duplicate_order,
    "fraud_phantom_vendor": chaos_fraud_phantom_vendor,
    "fraud_overpriced": chaos_fraud_overpriced,
    "fraud_unauthorized_approval": chaos_fraud_unauthorized_approval,
    "fraud_personal_delivery": chaos_fraud_personal_delivery,

    # Processos incompletos
    "incomplete_payment": chaos_incomplete_payment,
    "incomplete_shipping": chaos_incomplete_shipping,
    "incomplete_delivery": chaos_incomplete_delivery,
    "incomplete_product_missing": chaos_incomplete_product_missing,

    # Warehouse
    "warehouse_lost_transfer": chaos_warehouse_lost_transfer,
    "warehouse_quantity_mismatch": chaos_warehouse_quantity_mismatch,
    "warehouse_negative_stock": chaos_warehouse_negative_stock,
    "warehouse_missing_location": chaos_warehouse_missing_location,
    "warehouse_quality_fail": chaos_warehouse_quality_fail,
    "warehouse_cold_chain_break": chaos_warehouse_cold_chain_break,
    "warehouse_same_location_transfer": chaos_warehouse_same_location_transfer,
    "warehouse_missing_traceability": chaos_warehouse_missing_traceability,

    # Valores negativos
    "negative_cost": chaos_negative_cost,
    "negative_quantity": chaos_negative_quantity,
    "negative_freight": chaos_negative_freight,

    # Referências faltando
    "missing_order_ref": chaos_missing_order_ref,
    "missing_vendor_ref": chaos_missing_vendor_ref,
}


# ==================== MAIN FUNCTION ====================
def apply_data_quality_chaos(
    batch: List[Dict[str, Any]],
    chaos_rate: float = 0.05,
    allowed_chaos_types: List[str] = None,
    verbose: bool = False
) -> List[Dict[str, Any]]:
    """
    Aplica chaos engineering (5% padrão) nos dados gerados usando strategy pattern.

    Args:
        batch: Lista de dicionários com dados a serem inseridos
        chaos_rate: Percentual de registros a serem afetados (0.01 a 0.10)
        allowed_chaos_types: Lista de tipos de chaos permitidos (None = todos disponíveis)
        verbose: Se True, imprime estatísticas de chaos aplicado

    Returns:
        Lista de dicionários com anomalias injetadas

    Example:
        >>> orders = generate_sales_order_payloads(100, ...)
        >>> orders_with_chaos = apply_data_quality_chaos(
        ...     orders,
        ...     chaos_rate=0.05,
        ...     allowed_chaos_types=["duplicate", "fraud_fake_cpf", "incomplete_payment"],
        ...     verbose=True
        ... )
        >>> # Output: ✅ Chaos applied: 5 anomalies injected (3 duplicate, 1 fraud_fake_cpf, 1 incomplete_payment)
    """
    if not batch:
        return batch

    # Validar chaos_rate
    chaos_rate = max(0.01, min(chaos_rate, 0.10))

    num_chaos = max(1, int(len(batch) * chaos_rate))
    chaos_indices = random.sample(range(len(batch)), min(num_chaos, len(batch)))

    # Filtrar tipos de chaos disponíveis
    available_chaos_types = list(CHAOS_STRATEGIES.keys())
    chaos_types = allowed_chaos_types if allowed_chaos_types else available_chaos_types

    # Validar que todos os tipos solicitados existem
    invalid_types = set(chaos_types) - set(available_chaos_types)
    if invalid_types:
        print(f"⚠️ Invalid chaos types requested (ignored): {invalid_types}")
        chaos_types = [ct for ct in chaos_types if ct in available_chaos_types]

    if not chaos_types:
        print("⚠️ No valid chaos types available. Returning batch unchanged.")
        return batch

    chaos_applied = {}

    for idx in chaos_indices:
        chaos_type = random.choice(chaos_types)
        chaos_strategy = CHAOS_STRATEGIES[chaos_type]

        # Rastrear tipos de chaos aplicados
        chaos_applied[chaos_type] = chaos_applied.get(chaos_type, 0) + 1

        # Aplicar estratégia de chaos
        try:
            result = chaos_strategy(batch[idx])

            # Se a estratégia retornou um novo registro (duplicação), adicionar ao batch
            if result is not None:
                batch.append(result)

        except Exception as e:
            print(f"⚠️ Error applying chaos type '{chaos_type}': {e}")

    if verbose and chaos_applied:
        total_anomalies = sum(chaos_applied.values())
        chaos_summary = ", ".join([f"{count} {ctype}" for ctype, count in chaos_applied.items()])
        print(f"✅ Chaos applied: {total_anomalies} anomalies injected ({chaos_summary})")

    return batch


def generate_chaos_metadata(
    table_name: str,
    chaos_rate: float,
    chaos_types: List[str]
) -> Dict[str, Any]:
    """
    Gera metadados de chaos engineering para documentação.

    Args:
        table_name: Nome da tabela (ex: "sales.salesorder")
        chaos_rate: Taxa de chaos aplicada
        chaos_types: Lista de tipos de chaos injetados

    Returns:
        Dicionário com metadados estruturados

    Example:
        >>> metadata = generate_chaos_metadata(
        ...     "sales.salesorder",
        ...     0.05,
        ...     ["duplicate", "fraud_fake_cpf", "incomplete_payment"]
        ... )
    """
    return {
        "table": table_name,
        "chaos_engineering": {
            "enabled": True,
            "chaos_rate": chaos_rate,
            "chaos_types": chaos_types,
            "documentation": "See dbapi/api/seeder/CHAOS_DOCUMENTATION.md for details"
        }
    }


def get_available_chaos_types() -> List[str]:
    """Retorna lista de todos os tipos de chaos disponíveis."""
    return list(CHAOS_STRATEGIES.keys())
