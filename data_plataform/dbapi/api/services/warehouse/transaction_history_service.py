from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from api.repositories.warehouse.transaction_history_repository import TransactionHistoryRepository
from api.schemas.warehouse.warehouse_transactionhistory import TransactionHistorySchema
from api.models.warehouse.transactionhistory import TransactionHistoryModel

class TransactionHistoryService:
    def __init__(self, session: AsyncSession):
        self.repository = TransactionHistoryRepository(session)

    async def list_transactions(self, limit: int = 100) -> List[TransactionHistoryModel]:
        return await self.repository.get_all(limit=limit)

    async def process_bulk_transactions(self, transactions_data: List[TransactionHistorySchema]):
        """
        Bulk processing of transaction history.
        Converts Pydantic schemas into dictionaries ready for the Repository.
        """
        # Cleanups and preparations before the bulk insert
        processed_data = []
        for schema in transactions_data:
            # We use Pydantic v2's model_dump()
            data = schema.model_dump()

            # We remove IDs if they are autoincrement in the database (optional depending on the seeder)
            # data.pop("transaction_id", None)

            # traceability_info will trigger SQLAlchemy's automatic JSON serialization
            processed_data.append(data)
            
        await self.repository.bulk_insert(processed_data)
