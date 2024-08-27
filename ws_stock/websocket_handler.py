from fugle_marketdata import WebSocketClient, RestClient
import json
import asyncio
import os
import time
FUGLE_API_KEY = os.getenv("FUGLE_API_KEY")

class WebSocketHandler:
    def __init__(self, handle_data_callback):
        self.client = WebSocketClient(api_key=FUGLE_API_KEY)
        self.handle_data_callback = handle_data_callback

    def handle_message(self, message):
        data = json.loads(message)
        if data.get("event")=="data":
            aaa = data.get("data")
            # print(f"i send sth from ws.{aaa}")
            self.handle_data_callback(aaa)
        # print(f"print from we: {data}")
        # self.handle_data_callback(data)
             
    def handle_connect(self):
        print('connected')

    def handle_disconnect(self, code, message):
        print(f'disconnect: {code}, {message}')
        time.sleep(5)  # 延迟5秒后重新连接
        self.start()

    def handle_error(self, error):
        print(f'error from ws: {error}')
        self.client.stock.connect()

    def start(self):
        # client = WebSocketClient(api_key=FUGLE_API_KEY)
        stock = self.client.stock
        stock.on("connect", self.handle_connect)
        stock.on("message", self.handle_message)
        stock.on("disconnect", self.handle_disconnect)
        stock.on("error", self.handle_error)
        stock.connect()
        stock.subscribe({
            "channel": 'trades',
            "symbol": '2330',
        })
        stock.subscribe({
            "channel": 'trades',
            "symbol": '0050',
        })
        stock.subscribe({
            "channel": 'trades',
            "symbol": '00670L',
        })
        stock.subscribe({
            "channel": 'trades',
            "symbol": '2454',
        })
        stock.subscribe({
            "channel": 'trades',
            "symbol": '6115',
        })