import asyncpg
import os

USER = os.getenv("POSTGRE_USER")
PASSWORD = os.getenv("POSTGRE_PASSWORD")

async def create_pool():
    return await asyncpg.create_pool(
        user=USER,
        password=PASSWORD,
        database='pc_db',
        host='v5-postgre.cxu0oc6yqrfs.us-east-1.rds.amazonaws.com',
        port='5432',
        min_size=10,
        max_size=20
    )