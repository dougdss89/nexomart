import asyncio
import os
import random
import sys
import uuid
from datetime import datetime, timedelta

from api.config.seeder_config import get_ranges
from api.database import AsyncSessionLocal
from api.models.humanresources.employee import HumanResourcesEmployeeModel
from api.models.humanresources.jobrole import HumanResourcesJobRoleModel
from api.models.purchasing.purchaseorderaudit import PurchaseOrderAuditModel
from api.models.purchasing.purchaseorderheader import PurchaseOrderHeaderModel
from api.schemas.purchasing.purchasing_orderaudit import AuditAction, PurchaseOrderAudit
from api.seeder.chaos import apply_purchase_audit_chaos
from api.utils.cli_common import add_chaos_args, build_parser, bulk_copy, resolve_chaos_args
from api.utils.quarantine import get_quarantine
from sqlalchemy import func, or_, select

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

MANAGEMENT_KEYWORDS = ["gerente", "manager", "diretor", "director", "chief"]

# Widened 2026-09-05 (was 5-300 days back) so event_date spans multiple years.
EARLIEST_TRANSACTION_DATE = datetime(2020, 1, 1)


def get_purchase_order_audit(
    valid_header_ids: list[str],
    valid_employee_ids: list[int],
    manager_employee_ids: list[int] | None = None,
    chaos_rate: float = 0.05,
    apply_chaos: bool = True,
) -> list[dict]:
    """
    Generates the audit trail for each order following a realistic sequence
    (CREATED -> APPROVED/REJECTED -> CLOSED), preferring managers for approvals.
    """
    batch = []
    q = get_quarantine("purchasing.purchaseorderaudit")

    if not valid_header_ids or not valid_employee_ids:
        return batch

    approver_pool = manager_employee_ids or valid_employee_ids

    for header_id in valid_header_ids:
        requester_id = random.choice(valid_employee_ids)
        event_date = datetime.now() - timedelta(
            days=random.randint(5, (datetime.now() - EARLIEST_TRANSACTION_DATE).days)
        )

        sequence = [(AuditAction.CREATED, requester_id, "Pedido criado")]

        decision = random.choice([AuditAction.APPROVED, AuditAction.REJECTED])
        approver_id = random.choice(approver_pool)
        sequence.append(
            (
                decision,
                approver_id,
                "Pedido aprovado" if decision == AuditAction.APPROVED else "Pedido rejeitado",
            )
        )

        if decision == AuditAction.APPROVED and random.random() < 0.6:
            sequence.append((AuditAction.CLOSED, approver_id, "Pedido concluído"))

        for action, emp_id, obs in sequence:
            payload = {
                # audit_id is a placeholder only, for Pydantic validation (gt=0) — popped
                # below before insert. It's autoincrement in the database; setting a real
                # value here would restart at 1 on every run and collide with the PK from
                # previous runs, breaking the INSERT for the whole batch.
                "audit_id": 1,
                "purchaseorderheader_id": header_id,
                "action": action,
                "employee_id": emp_id,
                "obs": obs,
                "created_at": event_date,
                "updated_at": event_date,
                "is_active": True,
                "row_guid": uuid.uuid4(),
            }
            with q.row(payload):
                validated_audit = PurchaseOrderAudit(**payload).model_dump()
                validated_audit.pop("audit_id", None)
                batch.append(validated_audit)
            event_date += timedelta(hours=random.randint(5, 48))

    if apply_chaos:
        batch = apply_purchase_audit_chaos(
            batch, valid_employee_ids, manager_employee_ids, chaos_rate=chaos_rate
        )

    q.finish()
    return batch


async def seed_purchase_order_audit(chaos_rate: float = 0.05, apply_chaos: bool = True):
    """Runs the audit trail load consuming REAL orders and employees.

    Root-cause fix (2026-08-17, revised same day): first attempt used an anti-join
    (`LEFT JOIN ... WHERE purchaseorderaudit.purchaseorderheader_id IS NULL`) instead of
    loading both full ID columns into Python — correct, but still measured slow against ~1.4M
    accumulated headers: Postgres has no index that identifies *which* headers lack an audit
    trail, so a Nested Loop Anti Join still walks header rows roughly in physical/insertion
    order until it accumulates enough misses, i.e. it still scans through most of the
    *matched* rows first. purchaseorderheader_id can't be used as a high-water mark either
    (it's a random "PO-XXXXXXXXX" business ID, not sequential), so this uses `created_at`
    instead (indexed by AuditMixin on every table): "pending" = headers created after the
    last audit batch's created_at, a plain indexed range scan. Correct as long as a run either
    fully commits its audit batch or rolls back entirely on error (true here), and as long as
    audit-seeding always runs strictly after header-seeding within the same DAG execution
    (true per the DAG's facts→derived TaskGroup ordering).
    """
    fan_out_limit = get_ranges(
        schema="purchasing", table="purchasing.purchaseorderheader"
    ).max_count

    async with AsyncSessionLocal() as session:
        watermark = await session.scalar(select(func.max(PurchaseOrderAuditModel.created_at)))
        # Only headers that don't have an audit trail yet — reprocessing an already
        # audited header just duplicates events without violating any constraint (audit_id is now
        # autoincrement), but grows without bound on every DAG run for no reason.
        pending_stmt = (
            select(PurchaseOrderHeaderModel.purchaseorderheader_id)
            .order_by(PurchaseOrderHeaderModel.created_at)
            .limit(fan_out_limit)
        )
        if watermark is not None:
            pending_stmt = pending_stmt.where(PurchaseOrderHeaderModel.created_at > watermark)
        header_ids = (await session.execute(pending_stmt)).scalars().all()
        employee_ids = (
            (await session.execute(select(HumanResourcesEmployeeModel.employee_id))).scalars().all()
        )

        manager_rows = await session.execute(
            select(HumanResourcesEmployeeModel.employee_id)
            .join(
                HumanResourcesJobRoleModel,
                HumanResourcesEmployeeModel.jobrole_id == HumanResourcesJobRoleModel.jobrole_id,
            )
            .where(
                or_(
                    *[
                        HumanResourcesJobRoleModel.job_title.ilike(f"%{kw}%")
                        for kw in MANAGEMENT_KEYWORDS
                    ]
                )
            )
        )
        manager_ids = list(manager_rows.scalars().all())

        if not header_ids or not employee_ids:
            print("⚠️ No header pending audit (or employees empty)! Run the base seeders first.")
            return

        print(f"📣 Seeding audit trail for {len(header_ids)} orders | chaos={chaos_rate:.2%}")
        audits = get_purchase_order_audit(
            list(header_ids),
            list(employee_ids),
            manager_ids or None,
            chaos_rate=chaos_rate,
            apply_chaos=apply_chaos,
        )

        try:
            total = await bulk_copy(session, PurchaseOrderAuditModel, audits)
            print(f"✅ {total} audit records seeded successfully!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error seeding audit trail: {e}")
            raise RuntimeError(f"Purchase order audit seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for purchasing.purchaseorderaudit (fan-out of real headers)")
    add_chaos_args(parser, default_chaos_rate=0.05)
    args = parser.parse_args()
    chaos_rate, apply_chaos = resolve_chaos_args(args, default_chaos_rate=0.05)

    asyncio.run(seed_purchase_order_audit(chaos_rate=chaos_rate, apply_chaos=apply_chaos))
