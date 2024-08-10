from ws.websocket_handler import WebSocketHandler
from kaf.kafka_func import create_kafka_topic
from producer.producer import send_batch_to_kafka, add_to_batch
from consumer.consumer import create_consumer
import time

def main():
    time.sleep(10)
    create_kafka_topic('raw_data', num_partitions=1)
    create_kafka_topic('processed_data', num_partitions=1)

    ws_handler = WebSocketHandler(handle_data_callback=add_to_batch)
    ws_handler.start()

    send_batch_to_kafka('raw_data')
    create_consumer('raw_data')  ## 暫時留著，未來檢查ws輸入用

if __name__ == "__main__":
    main()
