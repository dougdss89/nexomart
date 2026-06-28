"""
Standalone seeder — product.productcategory em nexomarket_db
Usa psycopg2 (disponível via apache-airflow-providers-postgres)
"""
import uuid
import sys
from datetime import datetime, timedelta
import random

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError:
    print("psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

CATEGORIES = [
    "electronics", "home_appliance", "furniture", "clothing", "footwear",
    "beauty_personal_care", "health_wellness", "books", "toys_games",
    "sports_outdoors", "automotive", "grocery", "pet_supplies",
    "office_supplies", "garden_tools", "musical_instruments",
    "jewelry_watches", "home_improvement", "video_games",
]

PG_HOST = "pgprojeto"
PG_PORT = 5432
PG_DB   = "nexomarket_db"
PG_USER = "admin"
PG_PWD  = "admin"


def connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PWD,
    )


def setup_schema(conn):
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS product")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product.productcategory (
                category_id         SERIAL PRIMARY KEY,
                category_name       VARCHAR(50) NOT NULL UNIQUE,
                category_identifier UUID        NOT NULL DEFAULT gen_random_uuid(),
                created_date        TIMESTAMPTZ DEFAULT now()
            )
        """)
    conn.commit()
    print("Schema and table ensured.")


def seed(conn):
    simulated_date = datetime.now() - timedelta(days=random.randint(30, 365))
    rows = [
        (i + 1, name, str(uuid.uuid4()), simulated_date)
        for i, name in enumerate(CATEGORIES)
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO product.productcategory
                (category_id, category_name, category_identifier, created_date)
            VALUES %s
            ON CONFLICT (category_name) DO NOTHING
            """,
            rows,
        )
    conn.commit()
    print(f"Seeded {len(rows)} categories into product.productcategory (conflicts ignored).")


if __name__ == "__main__":
    conn = None
    try:
        conn = connect()
        setup_schema(conn)
        seed(conn)
        print("Seed completed successfully.")
    except Exception as e:
        print(f"Seed failed: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
    finally:
        if conn:
            conn.close()
