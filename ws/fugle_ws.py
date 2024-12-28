import asyncio
import websockets
import json
import os
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


FUGLE_API_KEY = os.getenv("FUGLE_API_KEY")
URL = "wss://api.fugle.tw/marketdata/v1.0/stock/streaming"

class AsyncWSHandler:
    def __init__(self, handle_data_callback):
        self.handle_data_callback = handle_data_callback

    async def authenticate(self, websocket):
        try:
            auth_message = {
                "event": "auth",
                "data": {
                    "apikey": FUGLE_API_KEY
                }
            }
            await websocket.send(json.dumps(auth_message))
            logging.info("Sent authentication message.")
            return True
        except Exception as e:
            logging.info(f"Authentication error: {e}")
            return False
        
    async def subscribe(self, websocket, subscribe_list):
        try:
            ws_subscribe_list = {
                "event": "subscribe",
                "data": {
                    "channel": "trades",
                    "symbols": subscribe_list
                }
            }
            await websocket.send(json.dumps(ws_subscribe_list))
        except Exception as e:
            logging.info(f"Subscription error: {e}")
    


    async def handle_message(self, data, msg_deques):
        try:
            # await self.handle_data_callback(data, msg_deques)
            data = data.get("data")  # 影該是這邊沒寫好，會吃到 Error handling message: 'list' object has no attribute 'get'
            if data:
                await self.handle_data_callback(data, msg_deques)
        except Exception as e:
            logging.info(f"Error handling message: {e}")

    async def start(self, msg_deques, subscribe_list):
        try:
            async with websockets.connect(URL) as websocket:
                if await self.authenticate(websocket):
                    async for message in websocket:
                        data = json.loads(message)

                        if data.get("event") == "authenticated":
                            logging.info("Authenticated successfully.")
                            await self.subscribe(websocket, subscribe_list)

                        await self.handle_message(data, msg_deques)
                        # await asyncio.sleep(0)

        except KeyboardInterrupt:
            logging.info("disconnect...")
        except Exception as e:
            logging.info(f"Connection error: {e}")

    async def disconnect(self):
        if self.websocket:
            await self.websocket.close()
            await self.websocket.wait_closed()
            logging.info("Disconnected from WebSocket.")