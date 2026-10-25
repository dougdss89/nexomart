import asyncio
import os
import sys

# Adjust path to import API modules
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from api.database import AsyncSessionLocal
from api.models.warehouse.transactiontype import WarehouseTransactionType
from api.schemas.warehouse.warehouse_transaction_type import ActionEffect, TransactionTypeSchema
from api.services.warehouse.transaction_type_service import TransactionTypeService
from api.utils.cli_common import build_parser, skip_if_already_seeded, stamp_dimension_history_objs


async def seed_transaction_types(count: int):
    """
    Seeder for transaction types (Master Data).
    """
    async with AsyncSessionLocal() as session:
        service = TransactionTypeService(session)

        # 1. Primary Transaction Types (Required for the business, per AdventureWorks)
        primary_types = [
            TransactionTypeSchema(
                transactiontype_id=1,
                transaction_code="W",
                transaction_name="Work Order",
                description="Transaction relating to a work order.",
                action_effect=ActionEffect.NEUTRAL,
            ),
            TransactionTypeSchema(
                transactiontype_id=2,
                transaction_code="S",
                transaction_name="Sales Order",
                description="Transaction relating to a sales order.",
                action_effect=ActionEffect.OUT,
            ),
            TransactionTypeSchema(
                transactiontype_id=3,
                transaction_code="P",
                transaction_name="Purchase Order",
                description="Transaction relating to a purchase order.",
                action_effect=ActionEffect.IN,
            ),
        ]

        # In case extra codes need to be generated when count > len(primary_types)
        # For Master Data, we generally keep only the real codes.
        to_seed = primary_types[:count] if count < len(primary_types) else primary_types

        if await skip_if_already_seeded(
            session, WarehouseTransactionType, label="Transaction types"
        ):
            return

        print(f"🚀 Starting load of {len(to_seed)} transaction types into the database...")

        stamp_dimension_history_objs(to_seed)  # backdate created_at so the landing spans 2020->now
        try:
            await service.seed_initial_data(to_seed)
            await session.commit()
            print(f"✅ Load finished successfully! Total: {len(to_seed)}")
        except Exception as e:
            await session.rollback()
            print(f"❌ Error during load: {e}")
            raise RuntimeError(f"Transaction type seeding aborted: {e}") from e


if __name__ == "__main__":
    parser = build_parser("Seeder for warehouse.transactiontype (fixed master data, no chaos)")
    parser.add_argument(
        "--count", type=int, default=10, help="Number of types to load (Master Data, max 3 today)"
    )
    args = parser.parse_args()

    asyncio.run(seed_transaction_types(args.count))
