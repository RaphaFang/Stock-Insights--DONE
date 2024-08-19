from fastapi.responses import JSONResponse
import asyncio
import redis
import redis.asyncio as aioredis
import json
import aiomysql 

headers = {"Content-Type": "application/json; charset=utf-8"}

async def handle_expired_keys(redis_pool, sql_pool):
    try:
        async with aioredis.Redis(connection_pool=redis_pool, decode_responses=True) as r:
            await r.config_set('notify-keyspace-events', 'Ex')
            pubsub = r.pubsub()
            await pubsub.subscribe('__keyevent@0__:expired')
            async for message in pubsub.listen():

                if message['type'] == 'message':
                    key = message['data'].decode('utf-8')
                    the_id, the_target = key.split(':')[1], key.split(':')[2]
                    if the_target == 'booking_trigger_key':
                        data = await r.get(f'user:{the_id}:booking')
                        await r.delete(f'user:{the_id}:booking')
                        if data:
                            booking_data = json.loads(data)
                            print(booking_data)
                            await write_to_sql(sql_pool, booking_data, the_id)
                        
    except asyncio.CancelledError:
        print("handle_expired_keys cancelled, correctly log out")
    except (aiomysql.Error, redis.RedisError) as err:
        return JSONResponse(status_code=500,content={"error": True, "message": str(err)},headers=headers)
    except (ValueError, Exception) as err:
        return JSONResponse(status_code=400,content={"error": True, "message": str(err)},headers=headers)


async def write_to_sql(sql_pool, bd, the_id):
    try:
        async with sql_pool.acquire() as connection:
            async with connection.cursor(aiomysql.DictCursor) as cursor:
                if bd:
                    await cursor.execute("""
                        INSERT INTO user_booking_tentative (
                            creator_id, attraction_id, name, address, image, date, time, price
                        )
                        SELECT %s, %s, %s, %s, %s, %s, %s, %s
                        ON DUPLICATE KEY UPDATE
                            attraction_id = VALUES(attraction_id), name = VALUES(name), address = VALUES(address), image = VALUES(image), date = VALUES(date), time = VALUES(time), price = VALUES(price);
                    """, (the_id, bd['attraction_id'], bd['name'], bd['address'], bd['image'], bd['date'], bd['time'], bd['price']))
                await connection.commit()

    except asyncio.CancelledError:
        print("handle_expired_keys cancelled, correctly log out")
    except (aiomysql.Error, redis.RedisError) as err:
        return JSONResponse(status_code=500,content={"error": True, "message": str(err)},headers=headers)
    except (ValueError, Exception) as err:
        return JSONResponse(status_code=400,content={"error": True, "message": str(err)},headers=headers)