# from fugle_marketdata import WebSocketClient, RestClient
# import json
# import asyncio
# import os
# FUGLE_API_KEY = os.getenv("FUGLE_API_KEY")

# class WebSocketHandler:
#     def __init__(self):
#         self.client = WebSocketClient(api_key=FUGLE_API_KEY)

#     def handle_message(self, message):
#         data = json.loads(message)
#         print(data)

#     def handle_connect(self):
#         print('connected')

#     def handle_disconnect(self , code, message):
#         print(f'disconnect: {code}, {message}')

#     def handle_error(self,error):
#         print(f'error from ws: {error}')

#     def start(self):
#         stock = self.client.stock
#         stock.on("connect", self.handle_connect)
#         stock.on("message", self.handle_message)
#         stock.on("disconnect", self.handle_disconnect)
#         stock.on("error", self.handle_error)
#         stock.connect()
#         stock.subscribe({
#             "channel": 'trades',
#             "symbol": '2330',
#         })
#         stock.subscribe({
#             "channel": 'trades',
#             "symbol": '0050',
#         })
#         stock.subscribe({
#             "channel": 'trades',
#             "symbol": '00670L',
#         })
#         stock.subscribe({
#             "channel": 'trades',
#             "symbol": '2454',
#         })
#         stock.subscribe({
#             "channel": 'trades',
#             "symbol": '6115',
#         })



# # ws= WebSocketHandler()
# # ws.start()