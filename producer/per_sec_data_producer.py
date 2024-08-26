from confluent_kafka import Consumer, Producer, KafkaError
import json
import threading

stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "6115": 4
}
consumer_config = {
    'bootstrap.servers': 'kafka_stack_kafka:9092',
    'group.id': 'send_group',
    'auto.offset.reset': 'earliest',
}
producer_config = {
    'bootstrap.servers': 'kafka_stack_kafka:9092',
    'acks': 'all', 
}

# def send_to_partitioned_topic(symbol, message):

consumer = Consumer(consumer_config)
consumer.subscribe(['kafka_per_sec_data'])
producer = Producer(producer_config)

def kafka_per_sec_data_producer():
    msg = consumer.poll(timeout=1.0)
    if msg:
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                pass
            else:
                print(msg.error())

        raw_message = json.loads(msg.value().decode('utf-8'))
        symbol = raw_message.get('symbol')
        par = stock_to_partition.get(symbol, 0)
                
        producer.produce('kafka_per_sec_data_partition', value=json.dumps(raw_message).encode('utf-8'), partition=par)
        producer.poll(0)
            # producer.flush()
    else:
        print("nothing at `kafka_per_sec_data`, nothing to send to kafka_per_sec_data_partition")
        
    producer.flush()
    threading.Timer(1.0, kafka_per_sec_data_producer).start()
                
    # except KeyboardInterrupt:
    #     pass
    # finally:
    #     consumer.close()