import uuid
import random
from datetime import datetime, timedelta

# Ajuste os imports para o seu caminho real
from api.schemas.purchase_audit import AuditAction

def get_purchase_order_audit(
    valid_header_ids: list[str],
    valid_employee_ids: list[int],
) -> list[dict]:
    """
    Gera registros de auditoria para pedidos de compra.
    """
    batch = []
    audit_id_counter = 1

    if not valid_header_ids or not valid_employee_ids:
        return batch
    
    #para cada pedido uma trilha de auditoria
    for header_id in valid_header_ids:

        employee_id = random.choice(valid_employee_ids)

        data_evento = datetime.now() - timedelta(days=random.randint(5, 300))
        
        batch.append({

            "audit_id": audit_id_counter,
            "purchaseorderheader_id": header_id,
            "action": random.choice(list(AuditAction)),
            "employee_id": employee_id,
            "obs": random.choice(["Pedido aprovado", "Pedido rejeitado", "Pedido cancelado", "Pedido concluído"]),

            "created_at": data_evento,
            "updated_at": data_evento,
            "is_active": True,
            "row_guid": uuid.uuid4()
        })
    
        audit_id_counter += 1
        
        sofreu_acao  = random.choice([True, False])
        if sofreu_acao:
            data_evento += timedelta(hours =random.randint(5, 48))

            manager_id = random.choice(valid_employee_ids)

            batch.append({
                "audit_id": audit_id_counter,
                "purchaseorderheader_id": header_id,
                "action": random.choice(list(AuditAction)),
                "employee_id": employee_id,
                "obs": random.choice(["Pedido aprovado", "Pedido rejeitado", "Pedido cancelado", "Pedido concluído"]),

                "created_at": data_evento,
                "updated_at": data_evento,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
        audit_id_counter += 1

    return batch
