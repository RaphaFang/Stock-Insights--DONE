# import threading
# import websocket
# def send_to_websocket(ws_url, data):
#     ws = websocket.create_connection(ws_url)
#     ws.send(data)
#     ws.close()

# def kafka_to_websocket(consumer, ws_url):
#     for message in consumer:
#         send_to_websocket(ws_url, message.value)

# def create_consumer_and_send_to_websocket(topic_name, ws_url):
#     consumer = KafkaConsumer(
#         topic_name,
#         bootstrap_servers=['localhost:9092'],
#         group_id='ws_group',  # group_id for the websocket consumer
#         auto_offset_reset='earliest'
#     )
#     kafka_to_websocket(consumer, ws_url)