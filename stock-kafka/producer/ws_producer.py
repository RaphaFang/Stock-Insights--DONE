import json
from datetime import datetime
import time
import asyncio
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def real_data_to_batch(data, msg_queue):
    symbol = data.get("symbol")
    if symbol in msg_queue:
        await msg_queue[symbol].put(data)

async def heartbeat_data_to_batch(msg_queue, stock_to_partition):
    try:
        while True:
            start = time.time()
            for symbol in stock_to_partition.keys():
                data = {
                    "symbol": symbol,
                    "type": "heartbeat",
                    "exchange": None,
                    "market": None,
                    "price": 0.0,
                    "size": 0,
                    "bid": 0.0,
                    "ask": 0.0,
                    "volume": 0,
                    "isContinuous": False,
                    "time": int(datetime.utcnow().timestamp() * 1000000),
                    "serial": "heartbeat_serial",
                    "id": "heartbeat_id",
                    "channel": "heartbeat_channel",
                    "yesterday_price": 200
                }
                await msg_queue[symbol].put(data)

            next_time = start + 1 - 0.00511
            sleep_time = max(0, next_time - time.time())
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logging.warning("asyncio.CancelledError: Heartbeat generation cancelled.")
    except Exception as e:
        logging.error(f"Error in generate_heartbeat_data: {e}")


async def create_producer():
    producer = None
    try:
        producer = AIOKafkaProducer(bootstrap_servers='10.0.1.138:9092')
        await producer.start()
        logging.info("Producer lunched, from ws_producer create_producer()")
        return producer
        
    except KafkaError as e:
        logging.error(f"KafkaError: {e}. Restart after 5 sec...")
        await asyncio.sleep(5)
    except Exception as e:
        logging.error(f"Exception: {e}. Restart after 5 sec...")
        await asyncio.sleep(5)

async def send_batch_to_kafka(topic, msg_queue, stock_to_partition, yesterday_price):
    try:
        producer = await create_producer()

        while True:
            for symbol, queue in msg_queue.items():
                while not queue.empty():
                    par = stock_to_partition[symbol]
                    msg = await queue.get()
                    msg["yesterday_price"] = yesterday_price
                    json_data = json.dumps(msg).encode('utf-8')
                    await producer.send(topic, partition=par, value=json_data)
            await asyncio.sleep(0) # 之前縮排到for裡面，導致睡的太頻繁卡住
                        
    except KafkaError as e:
        logging.error(f"KafkaError in send_batch_to_kafka: {e}")                
    except Exception as e:
        logging.error(f"Exception in send_batch_to_kafka: {e}")
    finally:
        await producer.stop()

        # async with AIOKafkaProducer(bootstrap_servers='kafka:9092') as producer:
        # 不能用async with 他會跑完馬上段開producer連結
        # producer.flush() 在 aiokafka 是隱式的，不需要顯式調用。但是send 不會立刻將資料傳遞，還是要用flush