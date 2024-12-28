import json
import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import KafkaError
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def create_consumer(topic):
    while True:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers='10.0.1.138:9092',
                group_id='new_testing_group',
                auto_offset_reset='earliest',
            )
            await consumer.start()
            return consumer

        except KafkaError as e:
            logging.info(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


async def create_consumer_by_partition(topic):
    try:
        consumer = await create_consumer(topic)
        async for message in consumer:
            raw = json.loads(message.value.decode("utf-8"))
            logging.info(raw)
            # TEMPORARY CHECKING "2330" data 
            # if raw.get("symbol")=="2330":
            #     logging.info(raw)
            # if raw.get("symbol")=="2330" and raw.get("MA_type")=="5_MA_data":
            #     logging.info(raw)
                        
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.info(f"Error occurred: {e}, retrying...")
        await asyncio.sleep(5)
    finally:
        await consumer.stop()