from confluent_kafka import Consumer, Producer, KafkaError
import json
import threading
import time


stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "6115": 4
}
consumer_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'send_group',
    'auto.offset.reset': 'earliest',
}
producer_config = {
    'bootstrap.servers': 'kafka:9092',
    'acks': 'all', 
}

# def send_to_partitioned_topic(symbol, message):
def create_consumer(consumer_config):
    while True:
        try:
            con = Consumer(consumer_config)
            print("Consumer build up, from kafka_per_sec_data_producer")
            return con
        except KafkaError as e:
            print(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_producer(producer_config):
    while True:
        try:
            prod = Producer(producer_config)
            print("Producer build up, from kafka_per_sec_data_producer")
            return prod
        except KafkaError as e:
            print(f"Kafka producer creation failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)



def kafka_per_sec_data_producer():
    consumer = create_consumer(consumer_config)
    consumer.subscribe(['kafka_per_sec_data'])
    producer = create_producer(producer_config)

    def loop_process():
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
            producer.flush()
        else:
            print("nothing at `kafka_per_sec_data`, nothing to send to kafka_per_sec_data_partition")
            
        producer.flush()
        threading.Timer(1.0, loop_process).start()
        
    loop_process()
                
    # except KeyboardInterrupt:
    #     pass
    # finally:
    #     consumer.close()