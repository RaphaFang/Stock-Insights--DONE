import asyncio
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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

async def send_to_partitioned_topic(symbol, message, producer):
    par = stock_to_partition.get(symbol, 0)
    await producer.send_and_wait('kafka_per_sec_data_partition', value=json.dumps(message).encode('utf-8'), partition=par)

async def kafka_per_sec_data_producer():
    consumer = AIOKafkaConsumer(
        'kafka_per_sec_data',
        bootstrap_servers=consumer_config['bootstrap.servers'],
        group_id=consumer_config['group.id'],
        auto_offset_reset=consumer_config['auto.offset.reset']
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=producer_config['bootstrap.servers']
    )

    await consumer.start()
    await producer.start()

    try:
        async for msg in consumer:
            raw_message = json.loads(msg.value.decode('utf-8'))
            symbol = raw_message.get('symbol')
            await send_to_partitioned_topic(symbol, raw_message, producer)

    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()
        await producer.stop()
