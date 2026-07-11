import asyncio
import os
import sys
import uuid
import random
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy import select, insert, or_
from api.database import AsyncSessionLocal
from api.schemas.purchasing.purchasing_orderaudit import AuditAction
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from api.models.purchasing.purchaseorderaudit import PurchaseOrderAuditModel
from api.seeder.chaos import apply_purchase_audit_chaos

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

MANAGEMENT_KEYWORDS = ["gerente", "manager", "diretor", "director", "chief"]


def get_purchase_order_audit(
    valid_header_ids: list[str],
    valid_employee_ids: list[int],
    manager_employee_ids: Optional[list[int]] = None,
) -> list[dict]:
    """
    Gera a trilha de auditoria de cada pedido seguindo uma sequência realista
    (CREATED -> APPROVED/REJECTED -> CLOSED), preferindo gerentes para aprovações.
    """
    batch = []
    audit_id_counter = 1

    if not valid_header_ids or not valid_employee_ids:
        return batch

    approver_pool = manager_employee_ids or valid_employee_ids

    for header_id in valid_header_ids:
        requester_id = random.choice(valid_employee_ids)
        data_evento = datetime.now() - timedelta(days=random.randint(5, 300))

        sequence = [(AuditAction.CREATED, requester_id, "Pedido criado")]

        decision = random.choice([AuditAction.APPROVED, AuditAction.REJECTED])
        approver_id = random.choice(approver_pool)
        sequence.append((
            decision, approver_id,
            "Pedido aprovado" if decision == AuditAction.APPROVED else "Pedido rejeitado",
        ))

        if decision == AuditAction.APPROVED and random.random() < 0.6:
            sequence.append((AuditAction.CLOSED, approver_id, "Pedido concluído"))

        for action, emp_id, obs in sequence:
            batch.append({
                "audit_id": audit_id_counter,
                "purchaseorderheader_id": header_id,
                "action": action,
                "employee_id": emp_id,
                "obs": obs,
                "created_at": data_evento,
                "updated_at": data_evento,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
            audit_id_counter += 1
            data_evento += timedelta(hours=random.randint(5, 48))

    return apply_purchase_audit_chaos(batch, valid_employee_ids, manager_employee_ids)


async def seed_purchase_order_audit():
    """Executa a carga da trilha de auditoria consumindo pedidos e funcionários REAIS."""
    async with AsyncSessionLocal() as session:
        header_ids = (await session.execute(select(PurchaseOrderHeaderModel.purchaseorderheader_id))).scalars().all()
        employee_ids = (await session.execute(select(HumanResourcesEmployeeModel.employee_id))).scalars().all()

        manager_rows = await session.execute(
            select(HumanResourcesEmployeeModel.employee_id)
            .join(HumanResourcesJobRoleModel, HumanResourcesEmployeeModel.jobrole_id == HumanResourcesJobRoleModel.jobrole_id)
            .where(or_(*[HumanResourcesJobRoleModel.job_title.ilike(f"%{kw}%") for kw in MANAGEMENT_KEYWORDS]))
        )
        manager_ids = list(manager_rows.scalars().all())

        if not header_ids or not employee_ids:
            print("⚠️ Headers ou Funcionários vazios! Rode os seeders base primeiro.")
            return

        print(f"📣 Semeando trilha de auditoria para {len(header_ids)} pedidos...")
        audits = get_purchase_order_audit(list(header_ids), list(employee_ids), manager_ids or None)

        try:
            stmt = insert(PurchaseOrderAuditModel).values(audits)
            await session.execute(stmt)
            await session.commit()
            print(f"✅ {len(audits)} registros de auditoria semeados com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro ao semear auditoria: {e}")


if __name__ == "__main__":
    asyncio.run(seed_purchase_order_audit())
