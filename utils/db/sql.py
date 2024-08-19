import os
import aiomysql
import asyncio
print(os.getenv('SQL_USER'))
print(os.getenv('SQL_PASSWORD'))

async def build_async_sql_pool():
    return await aiomysql.create_pool(
        # -----------------------------------------
        host="database-v5.cxu0oc6yqrfs.us-east-1.rds.amazonaws.com",
        user=os.getenv('SQL_USER'),
        password=os.getenv('SQL_PASSWORD'),
        # -----------------------------------------
        # host='localhost',
        # user=os.getenv('SQL_USER_LOCAL'),
        # password=os.getenv('SQL_PASSWORD_LOCAL'),
        # -----------------------------------------
        port=3306,
        db='basic_db',
        minsize=1,
        maxsize=10,
        loop=asyncio.get_event_loop()
    )