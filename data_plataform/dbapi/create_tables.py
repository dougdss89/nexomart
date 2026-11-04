"""
Setup script: creates schemas and all tables in nexomarket_db.
Usage: uv run python dbapi/create_tables.py
"""

import asyncio
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "dbapi"))

from api.config.base import Base

# Importar todos os models para registrar no metadata
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from configs.api.settings import settings

DATABASE_URL = settings.DATABASE_URL

SCHEMAS = ["product", "person", "sales", "warehouse", "humanresources", "marketing", "purchasing"]


async def setup():
    engine = create_async_engine(DATABASE_URL, echo=False)

    async with engine.begin() as conn:
        # 1. Create schemas
        for schema in SCHEMAS:
            await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        print(f"✅ {len(SCHEMAS)} schemas created")

        # 2. Create extensions
        await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'))
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        print("✅ Extensions: uuid-ossp, vector")

        # 3. Create all tables
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables created")

    # 4. Count created tables
    async with engine.connect() as conn:
        result = await conn.execute(
            text("""
            SELECT schemaname, COUNT(*) as n
            FROM pg_tables
            WHERE schemaname = ANY(:schemas)
            GROUP BY schemaname
            ORDER BY schemaname
        """),
            {"schemas": SCHEMAS},
        )
        rows = result.fetchall()
        total = sum(r.n for r in rows)
        for r in rows:
            print(f"   {r.schemaname}: {r.n} tables")
        print(f"\n✅ Total: {total} tables across {len(rows)} schemas")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(setup())
