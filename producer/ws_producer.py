import asyncio
import json
from collections import deque
from aiokafka import AIOKafkaProducer
from datetime import datetime

stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "6115": 4
}
kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'acks': 'all', 
    'retries': 5,   
    'retry.backoff.ms': 1000,
    'delivery.timeout.ms': 30000, 
    }
msg_deques = {symbol: deque() for symbol in stock_to_partition}


async def send_heartbeat(symbol, topic, producer):
    heartbeat_message = {
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
        "channel": "heartbeat_channel"
    }

    partition = stock_to_partition[symbol]
    json_data = json.dumps(heartbeat_message).encode('utf-8')
    await producer.send_and_wait(topic, value=json_data, partition=partition)
    # print(f"Heartbeat sent to topic {topic} for symbol {symbol}")


async def send_batch_to_kafka(topic):
    producer = AIOKafkaProducer(**kafka_config)
    await producer.start()
    try:
        while True:
            for symbol, deque in msg_deques.items():
                if deque:
                    batch = list(deque)
                    deque.clear()
                    for msg in batch:
                        partition = stock_to_partition[symbol]
                        json_data = json.dumps(msg).encode('utf-8')
                        await producer.send_and_wait(topic, value=json_data, partition=partition)
                else:
                    await send_heartbeat(producer, symbol, topic)
            await asyncio.sleep(1)
    finally:
        await producer.stop()

def add_to_batch(data):
    symbol = data.get("symbol")
    if symbol in msg_deques:
        msg_deques[symbol].append(data)



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