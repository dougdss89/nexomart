import uuid
import random
from datetime import datetime, timedelta
from decimal import Decimal
from faker import Faker

from api.schemas.purchasing.purchasing_orderheader import Status, AuditAction
fake = Faker('pt_BR')

def get_purchase_order_header(vendor_ids: list[int], employee_ids: list[int], num_records: int = 200) -> list[dict]:
    
    """
    Gera Pedidos de Compra (Header) com cálculos financeiros exatos 
    e trilha de auditoria JSONB realista.
    """

    batch = []

    STATUS = [Status.WAITING_APPROVAL, Status.APPROVED, Status.REJECTED, Status.CANCELED, Status.PENDING, Status.COMPLETED]
    
    for order_id in range(1, num_records + 1):
        
        #pedido entre 1 e três anos
        order_date = datetime.now() - timedelta(days=random.randint(1, 1095))
        
        #envio entre 1 e 15dias
        ship_date = order_date + timedelta(days= random.randint(1, 15))

        #sorteio do status do pedido
        status = random.choice(STATUS)

        if status == Status.COMPLETED:
            orderfinished_date = ship_date + timedelta(days= random.randint(1, 15))
        else:
            orderfinished_date = None

        #cálculo dos valores
        sub_total = Decimal(str(round(random.uniform(1000.0, 50000.0), 4)))
        tax_amt = Decimal(str(round(float(sub_total) * 0.12, 4))) # 12% de imposto
        freight = Decimal(str(round(random.uniform(50.0, 500.0), 4)))
        total_due = sub_total + tax_amt + freight

        audit_trail = [
            {
                "date": order_date.isoformat(),
                "action": AuditAction.CREATED.value,
                "employee_id": f"EMP-{random.choice(employee_ids)}",
                "comments": "Pedido de compra gerado pelo sistema"
            }]
        
        #adiciona histórico se o status mudou
        if status == Status.APPROVED:
            audit_trail.append({
                "date": order_date + timedelta(days= random.randint(1, 15)),
                "action": AuditAction.APPROVED,
                "employee_id": random.choice(employee_ids),
                "comments": "Pedido aprovado"
            })

        if status == Status.REJECTED:
            audit_trail.append({
                "date": order_date + timedelta(days= random.randint(1, 15)),
                "action": AuditAction.REJECTED,
                "employee_id": random.choice(employee_ids),
                "comments": "Pedido rejeitado"
            })

        if status == Status.CANCELED:
            audit_trail.append({
                "date": order_date + timedelta(days= random.randint(1, 15)),
                "action": AuditAction.CANCELED,
                "employee_id": random.choice(employee_ids),
                "comments": "Pedido cancelado"
            })

        if status == Status.COMPLETED:
            audit_trail.append({
                "date": order_date + timedelta(days= random.randint(1, 15)),
                "action": AuditAction.CLOSED,
                "employee_id": random.choice(employee_ids),
                "comments": "Pedido concluído"
            })

        # Gera o formato PO-XXXXXX (ex: PO-100001)
        po_header_id = f"PO-{100000 + order_id}"

        batch.append({
            "purchaseorderheader_id": po_header_id,
            "vendor_id": random.choice(vendor_ids),
            "employee_id": random.choice(employee_ids),

            "order_date": order_date,
            "ship_date": ship_date,
            "orderfinished_date": orderfinished_date,

            "sub_total": sub_total,
            "tax_amt": tax_amt,
            "freight": freight,
            "total_due": total_due,

            "status": status.value,
            "audit_trail": audit_trail,

            "created_at": order_date,
            "updated_at": order_date,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })

    return batch

if __name__ == "__main__":
    dummy_vendors = list(range(1, 51))
    dummy_employees = list(range(1, 11))
    pedidos_mock = get_purchase_order_header(vendor_ids=dummy_vendors, employee_ids=dummy_employees, num_records=10)
    
    print(f"Total de cabeçalhos de pedido gerados: {len(pedidos_mock)}")
    print("-" * 50)
    
    import json
    # Customizando o parser de data/decimal para imprimir no terminal
    
    print(json.dumps(pedidos_mock, indent=2, default=str))