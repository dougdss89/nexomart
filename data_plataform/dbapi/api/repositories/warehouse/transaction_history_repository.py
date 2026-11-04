from api.models.warehouse.transactionhistory import TransactionHistoryModel
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession


class TransactionHistoryRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_all(self, limit: int = 100) -> list[TransactionHistoryModel]:
        query = (
            select(TransactionHistoryModel)
            .order_by(TransactionHistoryModel.transaction_date.desc())
            .limit(limit)
        )
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def get_by_id(self, transaction_id: int) -> TransactionHistoryModel | None:
        query = select(TransactionHistoryModel).filter(
            TransactionHistoryModel.transaction_id == transaction_id
        )
        result = await self.session.execute(query)
        return result.scalar_one_or_none()

    async def bulk_insert(self, data_list: list[dict]):
        """
        Bulk insert via SQLAlchemy 2.0 core for performance.
        """
        if not data_list:
            return

        stmt = insert(TransactionHistoryModel).values(data_list)
        await self.session.execute(stmt)
        # Flush is necessary to trigger constraint errors, if any
        await self.session.flush()
