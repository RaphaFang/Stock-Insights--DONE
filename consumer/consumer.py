from confluent_kafka import Consumer, KafkaError
import time
import json

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'your_group_id', 
    'auto.offset.reset': 'latest',
    'session.timeout.ms': 30000, 
    'max.poll.interval.ms': 60000
}

def create_consumer(topic):
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic])

    print(f'Started consuming from topic: {topic}')

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


            raw =  json.loads(msg.value().decode("utf-8"))
            print(raw)


    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error occurred: {e}, retrying...")
        time.sleep(5)  # 等待5秒后重试
    finally:
        consumer.close()


# if __name__ == "__main__":
#     group_id = 'processed_data_group'
#     topic = 'processed_data'

#     # 启动多个消费者实例来处理同一个主题的不同分区
#     consumer1 = create_consumer(group_id)
#     consumer2 = create_consumer(group_id)

#     # 可以使用多线程或多进程来启动多个消费者实例
#     import threading
#     threading.Thread(target=consume_data, args=(consumer1, topic)).start()
#     threading.Thread(target=consume_data, args=(consumer2, topic)).start()