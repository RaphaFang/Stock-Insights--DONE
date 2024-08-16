import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
import json

async def consume_by_partition(topic, partition=None):
    consumer = AIOKafkaConsumer(
        bootstrap_servers='kafka:9092',
        group_id='per_sec_data_group',
        auto_offset_reset='latest',
        session_timeout_ms=30000,
        max_poll_interval_ms=60000
    )
    await consumer.start()

    try:
        if partition is not None:
            tp = TopicPartition(topic, partition)
            await consumer.assign([tp])
            print(f'Started consuming from topic: {topic}, partition: {partition}.')
        else:
            await consumer.subscribe([topic])
            print(f'Started consuming from topic: {topic}, but without partition.')
        
        async for msg in consumer:
            raw = json.loads(msg.value.decode("utf-8"))
            print(f"print from consumer: got msg {raw}")
    
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        await consumer.stop()

# 运行异步消费者
loop = asyncio.get_event_loop()
loop.run_until_complete(consume_by_partition('kafka_per_sec_data_partition', partition=0))
