# import asyncio
# import websockets
# import json
# import os

# FUGLE_API_KEY = os.getenv("FUGLE_API_KEY")
# URL = "wss://api.fugle.tw/marketdata/v1.0/stock/streaming"

# class WebSocketHandler:
#     def __init__(self, handle_data_callback):
#         self.handle_data_callback = handle_data_callback

#     async def authenticate(self, websocket):
#         try:
#             auth_message = {
#                 "event": "auth",
#                 "data": {
#                     "apikey": FUGLE_API_KEY
#                 }
#             }
#             await websocket.send(json.dumps(auth_message))
#             print("Sent authentication message.")
#             return True
#         except Exception as e:
#             print(f"Authentication error: {e}")
#             return False
        
#     async def connect(self):
#         try:
#             async with websockets.connect(URL) as websocket:
#                 if await self.authenticate(websocket):
#                     async for message in websocket:
#                         data = json.loads(message)

#                         if data.get("event") == "authenticated":
#                             print("Authenticated successfully.")
#                             await self.subscribe(websocket)

#                         self.handle_message(data)

#         except KeyboardInterrupt:
#             print("disconnect...")
#         except Exception as e:
#             print(f"Connection error: {e}")

#     async def subscribe(self, websocket):
#         try:
#             subscribe_list = {
#                 "event": "subscribe",
#                 "data": {
#                     "channel": "trades",
#                     "symbols": ["2330", "0050", "00670L", "2454", "6115"]
#                 }
#             }
#             await websocket.send(json.dumps(subscribe_list))
#             # print(f"Subscribed to symbols: {subscription_message['data']['symbols']}")
#         except Exception as e:
#             print(f"Subscription error: {e}")
    


#     def handle_message(self, message):
#         try:
#             self.handle_data_callback(message)
#         except Exception as e:
#             print(f"Error handling message: {e}")

#     # 等待接下來調整成非同步的callback
#     # async def handle_message(self, message):
#     #     try:
#     #         await self.handle_data_callback(message)
#     #     except Exception as e:
#     #         print(f"Error handling message: {e}")

#     async def disconnect(self):
#         if self.websocket:
#             await self.websocket.close()
#             await self.websocket.wait_closed()
#             print("Disconnected from WebSocket.")

# # async def main():
# #     handler = WebSocketHandler(handle_data_callback=lambda x: print(f"Received data: {x}"))
# #     await handler.connect()

# # asyncio.run(main())


# # 資訊先從connect進來
# # 接下來到handle_message()
# # 接下來到handle_message()裡面的 self.handle_data_callback
# # handle_data_callback這邊在連接到下一站的資料除理機制


# # connect(): Asynchronously opens a WebSocket connection.
# # recv(): Asynchronously receives messages from the WebSocket.
# # send(): Asynchronously sends messages to the WebSocket.
# # close(): Asynchronously closes the WebSocket connection with a proper closing handshake.
# # wait_closed(): Awaits the complete termination of the connection after closing.