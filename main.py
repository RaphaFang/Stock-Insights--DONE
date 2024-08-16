import asyncio
from ws.websocket_handler import WebSocketHandler
from kaf.kafka_func import create_kafka_topic
from producer.ws_producer import send_batch_to_kafka, add_to_batch
from producer.per_sec_data_producer import kafka_per_sec_data_producer
from consumer.consumer import create_consumer


ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)

async def main():
    # 建立全部的topic
    await create_kafka_topic('kafka_raw_data', num_partitions=5)
    await create_kafka_topic('kafka_per_sec_data', num_partitions=1)
    await create_kafka_topic('kafka_per_sec_data_partition', num_partitions=5)
    # await create_kafka_topic('kafka_processed_data', num_partitions=5)

    # 第1站，ws送資料到kafka_raw_data
    asyncio.to_thread(ws_handler.start)
    
    await send_batch_to_kafka('kafka_raw_data'),
    # 第2站，kafka_raw_data資料收到，送到kafka_per_sec_data
    # spark 資料已經處理好了

    # 第3站，spark處理的kafka_per_sec_data收到，送到kafka_per_sec_data_partition
    await kafka_per_sec_data_producer(),

    # 第4站，kafka_per_sec_data_partition資料送到spark作第二次處理
    await create_consumer('kafka_per_sec_data_partition', partition=0)


if __name__ == "__main__":
    asyncio.run(main())