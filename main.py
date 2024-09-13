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
create_topic('kafka_MA_data_aggregated', num_partitions=1)
# kafka_MA_data_aggregated
# create_topic('kafka_per_sec_data_partition', num_partitions=5)

async def async_main():
    # pool = await build_async_sql_pool()

    # 第1站，ws送資料到kafka_raw_data
    heartbeat_task = asyncio.create_task(heartbeat_data_to_batch(msg_queue, stock_to_partition))
    fugle_ws_handler = AsyncWSHandler(handle_data_callback=real_data_to_batch)
    websocket_task = asyncio.create_task(fugle_ws_handler.start(msg_queue, subscribe_list))
    producer_task = asyncio.create_task(send_batch_to_kafka("kafka_raw_data", msg_queue, stock_to_partition, 200))
    
    # 第2站，spark 接收 kafka_raw_data資料，送到kafka_per_sec_data

    # 第3.1站，kafka_per_sec_data，送spark
    # 第3.2站，kafka_per_sec_data，fastapi-ws 建立消費者直接出去
    # 第3.3站，kafka_per_sec_data，資料寫進DB
    # sec_to_sql_task = asyncio.create_task(consumer_to_queue("sec", per_sec_queue, 'kafka_per_sec_data'))
    # sec_writer_task = asyncio.create_task(queue_to_mysql('sec',per_sec_queue, pool))

    # 第4.1站，kafka_MA_data，fastapi-ws 建立消費者直接出去
    # 第4.2站，kafka_MA_data，資料寫進DB
    # ma_to_sql_task = asyncio.create_task(consumer_to_queue('MA',MA_queue, 'kafka_MA_data'))
    # ma_writer_task = asyncio.create_task(queue_to_mysql("MA", MA_queue, pool))

    # 測試區
    consumer_task = asyncio.create_task(create_consumer_by_partition("kafka_per_sec_data"))

    try: # , sec_to_sql_task, sec_writer_task, ma_to_sql_task, ma_writer_task
        await asyncio.gather(heartbeat_task, websocket_task, producer_task, consumer_task)
    except Exception as e:
        logging.error(f"Error in main: {e}")
    except KeyboardInterrupt:
        logging.info("STOP by control C.")


try:
    asyncio.run(async_main())
except Exception as e:
    logging.error(f"ERROR at async_main: {e}")

# -----------------------------------------------------------------------------------------------
# # async def async_main():
# #     pool = await build_async_sql_pool()
# #     MA_queue = Queue()
# #     per_sec_queue = Queue()

# #     await asyncio.gather(
# #         MA_data_consumer(queue=per_sec_queue, topic='kafka_per_sec_data', partition=None, prefix='sec'),
# #         mysql_writer(per_sec_queue, pool, 'sec'),

# #         MA_data_consumer(queue=MA_queue, topic='kafka_MA_data', partition=None, prefix='MA'),
# #         mysql_writer(MA_queue, pool, 'MA')
# #     )

# if __name__ == "__main__":
#     # asyncio.run(async_main())
#     sync_main()

# -----------------------------------------------------------------------------------------------
# from collections import deque   deque更加高效但是他只能支持同步

# def sync_main():
#     generate_heartbeat_data()

#     # 第1站，ws送資料到kafka_raw_data
#     ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
#     ws_handler.start()
#     send_batch_to_kafka('kafka_raw_data', 200)  # 增加了昨天報價的參數

#     # 第2站，kafka_raw_data資料收到，送到kafka_per_sec_data
#     # spark 資料已經處理好了

#     # 第3站，spark處理的kafka_per_sec_data收到，送到ws直接出去
#     # 已經送到 fastapi ws

#     # 第4站，kafka_per_sec_data_partition資料送到spark作第二次處理
#     # spark 資料已經處理好了，傳遞到kafka_MA_data

#     # 第5站，kafka_processed_data 資料送到 fastapi ws

#     # 測試區
#     create_consumer_by_partition('kafka_MA_data')





# def sync_main():
#     generate_heartbeat_data()

#     # 第1站，ws送資料到kafka_raw_data
#     ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
#     ws_handler.start()
#     send_batch_to_kafka('kafka_raw_data', 200)  # 增加了昨天報價的參數

#     # 第2站，kafka_raw_data資料收到，送到kafka_per_sec_data
#     # spark 資料已經處理好了

#     # 第3.1站，spark處理的kafka_per_sec_data收到，送到ws直接出去
#     # 已經送到 fastapi ws

#     # 第3.2站，spark處理的kafka_per_sec_data收到，送到kafka_per_sec_data_partition
#     # kafka_per_sec_data_producer()

#     # 第4站，kafka_per_sec_data_partition資料送到spark作第二次處理
#     # spark 資料已經處理好了，傳遞到kafka_MA_data

#     # 第5站，kafka_processed_data 資料送到 fastapi ws

#     # 測試區
#     create_consumer_by_partition('kafka_MA_data')