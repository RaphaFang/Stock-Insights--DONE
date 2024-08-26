from confluent_kafka import Consumer, KafkaError, TopicPartition
import time
import json

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'MA_kafka_side_group',
    'auto.offset.reset': 'latest',
    'session.timeout.ms': 30000,
    'max.poll.interval.ms': 60000
}

# kafka_config = {
#     'bootstrap.servers': 'kafka:9092',
#     'group.id': 'per_sec_data_partition_group',
#     'auto.offset.reset': 'latest',
#     'session.timeout.ms': 30000,
#     'max.poll.interval.ms': 60000
# }
def create_consumer(kafka_config):
    while True:
        try:
            con = Consumer(kafka_config)
            print("Consumer build up, from consumer_by_partition")
            return con
        except KafkaError as e:
            print(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_consumer_by_partition(topic, partition=None):
    # consumer = Consumer(kafka_config)
    consumer = create_consumer(kafka_config)
    if partition is not None:
        topic_partition = TopicPartition(topic, partition)
        consumer.assign([topic_partition])
        print(f'Started consuming from topic: {topic}, partition: {partition}.')
    else:
        consumer.subscribe([topic])
        print(f'Started consuming from topic: {topic}, but without partition.')

    try:
        while True:
            msgs = consumer.consume(num_messages=10, timeout=1.0)  
            if not msgs:
                continue
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                raw = json.loads(msg.value().decode("utf-8"))
                if raw.get("symbol")=="2330":
                    print(f"print from consumer: got msg {raw}")

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error occurred: {e}, retrying...")
        time.sleep(5)
    finally:
        consumer.close()