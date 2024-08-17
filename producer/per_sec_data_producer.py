from confluent_kafka import Consumer, Producer, KafkaError
import json

stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "6115": 4
}
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'kafka_per_sec_data_group',
    'auto.offset.reset': 'earliest',
}
producer_config = {
    'bootstrap.servers': 'kafka:9092',
}
consumer = Consumer(consumer_config)
producer = Producer(producer_config)
consumer.subscribe(['kafka_per_sec_data'])

def send_to_partitioned_topic(symbol, message):
    par = stock_to_partition.get(symbol, 0)
    producer.produce('kafka_per_sec_data_partition', value=json.dumps(message), partition=par)
    producer.flush()

def kafka_per_sec_data_producer():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            
            raw_message = json.loads(msg.value().decode('utf-8'))
            symbol = raw_message.get('symbol')
            send_to_partitioned_topic(symbol, raw_message)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()