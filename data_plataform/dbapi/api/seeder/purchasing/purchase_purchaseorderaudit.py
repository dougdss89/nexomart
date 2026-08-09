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
from api.seeder.cli_common import build_parser, add_chaos_args, resolve_chaos_args, insert_in_batches

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

MANAGEMENT_KEYWORDS = ["gerente", "manager", "diretor", "director", "chief"]


def get_purchase_order_audit(
    valid_header_ids: list[str],
    valid_employee_ids: list[int],
    manager_employee_ids: Optional[list[int]] = None,
    chaos_rate: float = 0.05,
    apply_chaos: bool = True,
) -> list[dict]:
    """
    Generates the audit trail for each order following a realistic sequence
    (CREATED -> APPROVED/REJECTED -> CLOSED), preferring managers for approvals.
    """
    batch = []

    if not valid_header_ids or not valid_employee_ids:
        return batch

    approver_pool = manager_employee_ids or valid_employee_ids

    for header_id in valid_header_ids:
        requester_id = random.choice(valid_employee_ids)
        event_date = datetime.now() - timedelta(days=random.randint(5, 300))

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
                # audit_id is deliberately left out of the payload: it's autoincrement in the database.
                # Setting it manually here (as it was before) restarts at 1 on every run and
                # collides with the PK from previous runs, breaking the INSERT for the whole batch.
                "purchaseorderheader_id": header_id,
                "action": action,
                "employee_id": emp_id,
                "obs": obs,
                "created_at": event_date,
                "updated_at": event_date,
                "is_active": True,
                "row_guid": uuid.uuid4()
            })
            event_date += timedelta(hours=random.randint(5, 48))

    if apply_chaos:
        batch = apply_purchase_audit_chaos(batch, valid_employee_ids, manager_employee_ids, chaos_rate=chaos_rate)

    return batch


async def seed_purchase_order_audit(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the audit trail load consuming REAL orders and employees."""
    async with AsyncSessionLocal() as session:
        # Only headers that don't have an audit trail yet — reprocessing an already
        # audited header just duplicates events without violating any constraint (audit_id is now
        # autoincrement), but grows without bound on every DAG run for no reason.
        already_audited = set(
            (await session.execute(select(PurchaseOrderAuditModel.purchaseorderheader_id.distinct()))).scalars().all()
        )
        all_header_ids = (await session.execute(select(PurchaseOrderHeaderModel.purchaseorderheader_id))).scalars().all()
        header_ids = [h for h in all_header_ids if h not in already_audited]
        employee_ids = (await session.execute(select(HumanResourcesEmployeeModel.employee_id))).scalars().all()

        manager_rows = await session.execute(
            select(HumanResourcesEmployeeModel.employee_id)
            .join(HumanResourcesJobRoleModel, HumanResourcesEmployeeModel.jobrole_id == HumanResourcesJobRoleModel.jobrole_id)
            .where(or_(*[HumanResourcesJobRoleModel.job_title.ilike(f"%{kw}%") for kw in MANAGEMENT_KEYWORDS]))
        )
        manager_ids = list(manager_rows.scalars().all())

        if not header_ids or not employee_ids:
            print("⚠️ No header pending audit (or employees empty)! Run the base seeders first.")
            return

        print(f"📣 Seeding audit trail for {len(header_ids)} orders | chaos={chaos_rate:.2%}")
        audits = get_purchase_order_audit(list(header_ids), list(employee_ids), manager_ids or None, chaos_rate=chaos_rate, apply_chaos=apply_chaos)

        try:
            total = await insert_in_batches(session, PurchaseOrderAuditModel, audits)
            print(f"✅ {total} audit records seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding audit trail: {e}")


if __name__ == "__main__":
    parser = build_parser("Seeder for purchasing.purchaseorderaudit (fan-out of real headers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_purchase_order_audit(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
