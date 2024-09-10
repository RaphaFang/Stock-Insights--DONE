import json
import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def create_consumer(topic):
    while True:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers='10.0.1.138:9092',
                group_id='testing_group',
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
            if raw.get("symbol")=="2330":
                logging.info(f"{raw}")
                        
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.info(f"Error occurred: {e}, retrying...")
        await asyncio.sleep(5)
    finally:
        await consumer.stop()

# -----------------------------------------------------------------------------------------------
    # config = {
    #     'bootstrap.servers': 'kafka:9092',
    #     'group.id': 'testing_group',
    #     'auto.offset.reset': 'latest',
    #     'session.timeout.ms': 30000,
    #     'max.poll.interval.ms': 60000
    # }
# async def create_consumer(topic):
#     while True:
#         try:
#             consumer = AIOKafkaConsumer(
#                 topic,
#                 bootstrap_servers='kafka:9092',
#                 group_id='testing_group',
#                 auto_offset_reset='earliest',
#             )
#             await consumer.start()
#             return consumer
        
#         except KafkaError as e:
#             logging.info(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
#             await asyncio.sleep(5)


# async def create_consumer_by_partition(topic):
#     try:
#         consumer = await create_consumer(topic)

#         # if partition is not None:
#         #     await consumer.assign([(topic, partition)])
#         #     print(f'Started consuming from topic: {topic}, partition: {partition}.')
#         # else:
#         #     await consumer.subscribe([topic])
#         #     print(f'Started consuming from topic: {topic}, but without partition.')


#         async for message in consumer:
#             raw = json.loads(message.value.decode("utf-8"))
#             logging.info(f"接收消息: \n{raw}")

#         # while True:
#         #     result = await consumer.getmany(timeout_ms=1000)
#         #     for tp, messages in result.items():
#         #         for msg in messages:
#         #             if msg.error():
#         #                 print(msg.error())
#         #                 break
#         #             raw = json.loads(msg.value.decode("utf-8"))
#         #             logging.info(f"接收消息: \n{raw}")
#         #     await asyncio.sleep(0)
                        
#     except KeyboardInterrupt:
#         print("Consumer stopped by user.")
#     except Exception as e:
#         print(f"Error occurred: {e}, retrying...")
#         await asyncio.sleep(5)
#     finally:
#         await consumer.stop()
# -----------------------------------------------------------------------------------------------

# def create_consumer(kafka_config):
#     while True:
#         try:
#             con = Consumer(kafka_config)
#             print("Consumer build up, from consumer_by_partition")
#             return con
#         except KafkaError as e:
#             print(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
#             time.sleep(5)
        
# def create_consumer_by_partition(config, topic, partition=None):
#     consumer = create_consumer(config, topic)

#     if partition is not None:
#         topic_partition = TopicPartition(topic, partition)
#         consumer.assign([topic_partition])
#         print(f'Started consuming from topic: {topic}, partition: {partition}.')
#     else:
#         consumer.subscribe([topic])
#         print(f'Started consuming from topic: {topic}, but without partition.')

#     try:
#         while True:
#             msgs = consumer.consume(num_messages=10, timeout=1.0)  
#             if not msgs:
#                 continue
#             for msg in msgs:
#                 if msg.error():
                        # if msg.error().code() == KafkaError._PARTITION_EOF:
                        #     continue
                        # else:
#                           print(msg.error())
#                           break

#                 raw = json.loads(msg.value().decode("utf-8"))
#                 if raw.get("symbol")=="2330":
#                     print(f"print from consumer: got msg {raw}")

#     except KeyboardInterrupt:
#         pass
#     except Exception as e:
#         print(f"Error occurred: {e}, retrying...")
#         time.sleep(5)
#     finally:
#         consumer.close()