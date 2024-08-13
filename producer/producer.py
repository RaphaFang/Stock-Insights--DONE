from confluent_kafka import Producer
import threading
import json
from collections import deque

stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "2603": 4
}

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'acks': 'all',  # 確保消息被所有副本接收
    'retries': 5,   # 重試次數
    'retry.backoff.ms': 1000,
    'delivery.timeout.ms': 30000,  # 增加超時時間到 30 秒
    }
producer = Producer(kafka_config)

msg_deque = deque()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_batch_to_kafka(topic):
    if msg_deque:
        batch = list(msg_deque)
        msg_deque.clear()
        for msg in batch:
            symbol = msg.get("symbol")
            par = stock_to_partition.get(symbol)

            if par is not None:
                print(f"Symbol: {symbol}, Partition : {par}")
                json_data = json.dumps(msg).encode('utf-8')  # 這邊一定要用bytes-like，也就是壓成json，再壓成字串
                producer.produce(topic, partition=par, value=json_data, callback=delivery_report)
        producer.poll(0)
        producer.flush()
    threading.Timer(1.0, send_batch_to_kafka, [topic]).start()

def add_to_batch(data):
    msg_deque.append(data)


# ------------------------------------------------------------
# 這個鎖的用意是處理，可能我資料打到kafka的時候，還有新資料加進去message_batch這個list
# 而這會導致資料不見
# !還是有看到建議說，要使用真正的對列
# message_batch = []
# batch_lock = threading.Lock() 
    
