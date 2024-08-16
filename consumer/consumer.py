import asyncio
from aiokafka import AIOKafkaConsumer
import json

async def create_consumer(topic):
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers='kafka:9092',
        group_id='testing_group',
        auto_offset_reset='latest',
        session_timeout_ms=30000,
        max_poll_interval_ms=60000
    )
    await consumer.start()
    print(f'Started consuming from topic: {topic}')

    try:
        async for msg in consumer:
            raw = json.loads(msg.value.decode("utf-8"))
            print(f"print from consumer: got msg {raw}")
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error occurred: {e}, retrying...")
        await asyncio.sleep(5) 
    finally:
        await consumer.stop()


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