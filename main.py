from ws_stock.websocket_handler import WebSocketHandler
from kaf.kafka_topic_create import create_kafka_topic

from producer.ws_producer import send_batch_to_kafka, add_to_batch, generate_heartbeat_data
from producer.per_sec_data_producer import kafka_per_sec_data_producer

# from consumer.consumer import create_consumer
from consumer.consumer_by_partition import create_consumer_by_partition

import time


create_kafka_topic('kafka_raw_data', num_partitions=5)
create_kafka_topic('kafka_per_sec_data', num_partitions=1)
create_kafka_topic('kafka_per_sec_data_partition', num_partitions=5)
create_kafka_topic('kafka_MA_data', num_partitions=1)

time.sleep(5) 

def main():
    generate_heartbeat_data()

    # 第1站，ws送資料到kafka_raw_data
    ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
    ws_handler.start()
    send_batch_to_kafka('kafka_raw_data')

    # 第2站，kafka_raw_data資料收到，送到kafka_per_sec_data
    # spark 資料已經處理好了

    # 第3.1站，spark處理的kafka_per_sec_data收到，送到ws直接出去
    # 已經送到 fastapi ws

    # 第3.2站，spark處理的kafka_per_sec_data收到，送到kafka_per_sec_data_partition
    # kafka_per_sec_data_producer()

    # 第4站，kafka_per_sec_data_partition資料送到spark作第二次處理
    # spark 資料已經處理好了，傳遞到kafka_MA_data

    # 第5站，kafka_processed_data 資料送到 fastapi ws

    # 測試區
    # create_consumer('kafka_per_sec_data')
    # create_consumer('kafka_per_sec_data_partition')
    create_consumer_by_partition('kafka_raw_data')
    # create_consumer_by_partition('kafka_per_sec_data_partition', partition=0)

if __name__ == "__main__":
    main()