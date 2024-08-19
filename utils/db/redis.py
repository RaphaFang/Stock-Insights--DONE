import os
import redis.asyncio as aioredis 

async def build_async_redis_pool():
    r_pool = aioredis.ConnectionPool.from_url(
        'redis://tdt-redis-1:6379/0',
        # 'redis://localhost:6379/0',
        password=os.getenv('REDIS_PASSWORD', None),
        max_connections=10
    )
    return r_pool