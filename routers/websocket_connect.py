from fastapi import APIRouter, WebSocket
from fastapi.responses import JSONResponse
import asyncio
# from kafka import KafkaConsumer
from aiokafka import AIOKafkaConsumer

router = APIRouter()
queue = asyncio.Queue()
headers = {"Content-Type": "application/json; charset=utf-8"}

# async def kafka_consumer_loop(topic_name):
#     consumer = KafkaConsumer(
#         topic_name,
#         bootstrap_servers=['kafka:9092'],
#         group_id='ws_group',
#         auto_offset_reset='earliest'
#     )
#     for message in consumer:
#         await queue.put(message.value.decode('utf-8'))



async def kafka_consumer_loop(topic_name):
    consumer = AIOKafkaConsumer(
        topic_name,
        bootstrap_servers='kafka:9092',
        group_id='ws_group',
        auto_offset_reset='earliest',
        loop=asyncio.get_event_loop()
    )
    await consumer.start()
    try:
        async for message in consumer:
            await queue.put(message.value.decode('utf-8'))
    finally:
        await consumer.stop()


@router.websocket("/ws/data")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    while True:
        # data = "Hello WebSocket"  # Placeholder data
        # await ws.send_text(data)
        data = await queue.get()
        if data:
            await ws.send_text(data)


    # try:
    #     async def search_mrts(request):
    #         sql_pool = request.state.async_sql_pool 
    #         async with sql_pool.acquire() as connection:
    #             async with connection.cursor() as cursor:
    #                 await cursor.execute("SELECT mrt, COUNT(DISTINCT name) as count FROM processed_data WHERE mrt IS NOT NULL AND mrt != '' GROUP BY mrt ORDER BY count DESC;") 
    #                 mrts_counted = await cursor.fetchall()
    #                 return {"data":[n[0] for n in mrts_counted]}
    #     result = await search_mrts(request)
    #     return JSONResponse(status_code=200,content=result, headers=headers)

    # except aiomysql.Error as err:
    #     return JSONResponse(status_code=500,content={"error": True, "message": str(err)},headers=headers)
    # except (Exception) as err:
    #     return JSONResponse(status_code=500,content={"error": True, "message": str(err)},headers=headers)