from confluent_kafka import Producer
import threading
import json

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    # 'client.id': 'your_client_id',
}
producer = Producer(kafka_config)

message_batch = []
batch_lock = threading.Lock() 
# 這個鎖的用意是處理，可能我資料打到kafka的時候，還有新資料加進去message_batch這個list
# 而這會導致資料不見
# !還是有看到建議說，要使用真正的對列
# ------------------------------------------------------------

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_batch_to_kafka(topic):
    global message_batch
    with batch_lock: 
        if message_batch:
            for msg in message_batch:
                symbol = msg.get("symbol")
                json_data = json.dumps(msg).encode('utf-8')
                # 這邊一定要用bytes-like，也就是壓成json，再壓成字串
                producer.produce(topic, key=str(symbol), value=json_data, callback=delivery_report)
            producer.poll(0)
            message_batch.clear()

    threading.Timer(1.0, send_batch_to_kafka, [topic]).start()

def add_to_batch(data):
    global message_batch
    with batch_lock: 
        message_batch.append(data)

