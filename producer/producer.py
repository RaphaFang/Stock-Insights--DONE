from confluent_kafka import Producer
import threading
import json
from collections import deque


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

# def get_empty_data():
#     return {
#         "symbol": symbol,
#         "timestamp": current_timestamp(),  # 確保時間戳與資料對應
#         "trade_volume": 0
#     }    

def send_batch_to_kafka(topic):

    if msg_deque:
        batch = list(msg_deque)
        msg_deque.clear()
        for msg in batch:
            symbol = msg.get("symbol")    #
            # event = msg.get("event")
            print(f"Symbol: {symbol}")
            # key = f"{symbol}_abcdefg"

            json_data = json.dumps(msg).encode('utf-8')  # 這邊一定要用bytes-like，也就是壓成json，再壓成字串
            producer.produce(topic, key=str(symbol), value=json_data, callback=delivery_report)
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
    
