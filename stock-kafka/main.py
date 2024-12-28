import asyncio
import logging
from kaf.topic_creater import create_topic
from producer.ws_producer import send_batch_to_kafka, real_data_to_batch, heartbeat_data_to_batch
from ws.fugle_ws import AsyncWSHandler
from sql.sql_consumer import build_async_sql_pool, consumer_to_queue, queue_to_mysql
from consumer.consumer_by_partition import create_consumer_by_partition

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 後續調整成aiologger
stock_to_partition = {
    "2330": 0,
    "0050": 1,
    "00670L": 2,
    "2454": 3,
    "6115": 4
}
subscribe_list = ["2330", "0050", "00670L", "2454", "6115"]
# ['2888', '00632R', '00680L', '00929', '2317', '00715L']
msg_queue = {symbol: asyncio.Queue() for symbol in stock_to_partition}

MA_queue = asyncio.Queue()
per_sec_queue = asyncio.Queue()

create_topic('kafka_raw_data', num_partitions=5)
create_topic('kafka_per_sec_data', num_partitions=1)
create_topic('kafka_MA_data', num_partitions=1)

# TESTING TOPIC
# create_topic('kafka_MA_data_aggregated', num_partitions=1)


async def async_main():
    pool = await build_async_sql_pool()

    # step1, send data collected by ws to kafka topic (kafka_raw_data)
    heartbeat_task = asyncio.create_task(heartbeat_data_to_batch(msg_queue, stock_to_partition))
    fugle_ws_handler = AsyncWSHandler(handle_data_callback=real_data_to_batch)
    websocket_task = asyncio.create_task(fugle_ws_handler.start(msg_queue, subscribe_list))
    producer_task = asyncio.create_task(send_batch_to_kafka("kafka_raw_data", msg_queue, stock_to_partition, 200))
    
    # step2, spark receives data from topic(kafka_raw_data), processing and sending to topic(kafka_per_sec_data)

    # step3.1, spark receives topic(kafka_per_sec_data), for aggregating MA data
    # step3.2, fastapi-ws receives topic(kafka_per_sec_data), send the data to front-end
    # step3.3, receiving data from topic(kafka_per_sec_data), writing them to DB
    sec_to_sql_task = asyncio.create_task(consumer_to_queue("sec", per_sec_queue, 'kafka_per_sec_data'))
    sec_writer_task = asyncio.create_task(queue_to_mysql('sec',per_sec_queue, pool))

    # step4.1, fastapi-ws receives topic(kafka_MA_data), send the data to front-end
    # step4.2, receiving data from topic(kafka_MA_data), writing them to DB
    ma_to_sql_task = asyncio.create_task(consumer_to_queue('MA',MA_queue, 'kafka_MA_data'))
    ma_writer_task = asyncio.create_task(queue_to_mysql("MA", MA_queue, pool))

    # TEST ZOON
    # consumer_task = asyncio.create_task(create_consumer_by_partition("kafka_MA_data_aggregated"))

    try:
        # await asyncio.gather(heartbeat_task, websocket_task, producer_task, consumer_task)
        await asyncio.gather(heartbeat_task, websocket_task, producer_task, sec_to_sql_task, sec_writer_task, ma_to_sql_task, ma_writer_task)
    except Exception as e:
        logging.error(f"Error in main: {e}")
    except KeyboardInterrupt:
        logging.info("STOP by control C.")

try:
    asyncio.run(async_main())
except Exception as e:
    logging.error(f"ERROR at async_main: {e}")