import asyncio
from postgre_pool import create_pool

from ws.websocket_handler import WebSocketHandler
from sp.spark_application_first import SparkHandler

from kaf.kafka_func import create_kafka_topic
from producer.producer import send_batch_to_kafka, add_to_batch
from consumer.consumer import create_consumer

import signal


def shutdown_handler(spark_handler):
    def handler(signum, frame):
        print(" -> -> -> Shutting down -> -> ->")
        spark_handler.stop()
        exit(0)
    return handler

def main():
    create_kafka_topic('raw_data', num_partitions=5)
    create_kafka_topic('processed_data', num_partitions=5)


    ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
    ws_handler.start()

    send_batch_to_kafka('raw_data')
    create_consumer('processed_data')  ## 暫時留著，未來檢查ws輸入用

    spark_handler = SparkHandler()
    signal.signal(signal.SIGINT, shutdown_handler)    # 透過Ctrl+C 終止程式時觸發
    signal.signal(signal.SIGTERM, shutdown_handler)   # 通用的終止訊號
    try:
        spark_handler.process_data()
    except KeyboardInterrupt:
        spark_handler.stop()

if __name__ == "__main__":
    main()
