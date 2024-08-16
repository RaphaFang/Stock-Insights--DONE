from ws.websocket_handler import WebSocketHandler
from kaf.kafka_func import create_kafka_topic
from producer.producer import send_batch_to_kafka, add_to_batch
from consumer.consumer import create_consumer
from producer.per_sec_data_producer import kafka_per_sec_data_producer
# import time

def main():
    # 建立全部的topic
    create_kafka_topic('kafka_raw_data', num_partitions=5)
    create_kafka_topic('kafka_per_sec_data', num_partitions=1)
    create_kafka_topic('kafka_per_sec_data_partition', num_partitions=5)
    # create_kafka_topic('kafka_processed_data', num_partitions=5)

    # 第1站，ws送資料到kafka_raw_data
    ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
    ws_handler.start()
    send_batch_to_kafka('kafka_raw_data')

    # 第2站，kafka_raw_data資料收到，送到kafka_per_sec_data
    # spark 資料已經處理好了

    # 第3站，spark處理的kafka_per_sec_data收到，送到kafka_per_sec_data_partition
    kafka_per_sec_data_producer()

    # 第4站，kafka_per_sec_data_partition資料送到spark作第二次處理
    create_consumer('kafka_per_sec_data_partition', partition=0)

if __name__ == "__main__":
    main()