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
        producer = AIOKafkaProducer(bootstrap_servers='kafka:9092')
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






# ------------------------------------------------------------
# stock_to_partition = {
#     "2330": 0,
#     "0050": 1,
#     "00670L": 2,
#     "2454": 3,
#     "6115": 4
# }
# msg_queue = {symbol: deque() for symbol in stock_to_partition}

# def create_producer():
#     while True:
#         try:
#             producer = Producer({'bootstrap.servers': 'kafka:9092'})
#             print("Producer build up, from ws_producer")
#             return producer
#         except KafkaError as e:
#             print(f"Kafka producer creation failed: {e}. Retrying in 5 seconds...")
#             time.sleep(5)
    

# def generate_heartbeat_data(msg_queue, stock_to_partition):
#     start = time.time()
#     for symbol in stock_to_partition.keys():
#         data = {
#             "symbol": symbol,
#             "type": "heartbeat",
#             "exchange": None,
#             "market": None,
#             "price": 0.0,
#             "size": 0,
#             "bid": 0.0,
#             "ask": 0.0,
#             "volume": 0,
#             "isContinuous": False,
#             "time": int(datetime.utcnow().timestamp() * 1000000),  
#             "serial": "heartbeat_serial",
#             "id": "heartbeat_id",
#             "channel": "heartbeat_channel",
#             "yesterday_price":200
#         }
#         msg_queue[symbol].append(data)

#     next = start + 1 - 0.00511
#     sleep_time = max(0, next - time.time())
#     threading.Timer(sleep_time, generate_heartbeat_data).start()

# def send_batch_to_kafka(topic, msg_queue, stock_to_partition, yesterday_price):
#     producer = create_producer()

#     def loop_process():
#         start = time.time()
#         for symbol, deque in msg_queue.items():
#             if deque:
#                 batch = list(deque)
#                 deque.clear()
#                 for msg in batch:
#                     par = stock_to_partition[symbol]
#                     msg["yesterday_price"]=yesterday_price
#                     json_data = json.dumps(msg).encode('utf-8')
#                     producer.produce(topic, partition=par, value=json_data) #, callback=delivery_report

#         producer.poll(0)
#         producer.flush()
#         next = start + 1
#         sleep_time = max(0, next - time.time())
#         threading.Timer(sleep_time, loop_process, [topic, yesterday_price]).start()

#     loop_process()

# ------------------------------------------------------------
# 這個鎖的用意是處理，可能我資料打到kafka的時候，還有新資料加進去message_batch這個list
# 而這會導致資料不見
# !還是有看到建議說，要使用真正的對列
# message_batch = []
# batch_lock = threading.Lock() 
    
# ------------------------------------------------------------
# msg_deque = deque()

# def send_batch_to_kafka(topic):
#     if msg_deque:
#         batch = list(msg_deque)
#         msg_deque.clear()
#         for msg in batch:
#             symbol = msg.get("symbol")
#             par = stock_to_partition.get(symbol)

#             if par is not None:
#                 print(f"Symbol: {symbol}, Partition : {par}")
#                 json_data = json.dumps(msg).encode('utf-8')  # 這邊一定要用bytes-like，也就是壓成json，再壓成字串
#                 producer.produce(topic, partition=par, value=json_data, callback=delivery_report)
#         producer.poll(0)
#         producer.flush()
#     threading.Timer(1.0, send_batch_to_kafka, [topic]).start()

# def add_to_batch(data):
#     msg_deque.append(data)