# 📈 Stock-Insights

Stock Insight is a service designed for investors who want real-time information on Taiwan's 5MA, 10MA, and 15MA stock data.

The raw data is retrieved from [Fugle's](https://developer.fugle.tw/)
WebSocket API and processed through a Kafka pipeline deployed across AWS EC2 instances. The project aggregates trades occurring within 1 second into actual trading information per second using Spark, and further processes this data into moving averages (MA) for 5MA, 10MA, and 15MA by Spark. The final MA data and per second data are then delivered to the front-end via a WebSocket API.

The project is deployed across three separate AWS EC2 instances, demonstrating the ability to maintain a Kafka pipeline across multiple cloud instances. It also highlights my expertise in **Kafka**, **Spark**, **WebSocket** and proficiency with **AWS services**. The complete development and learning period for this project spanned from Augest to September 2024.

## 🎥 Demo

![the gif demo for Stock-Insights project](https://github.com/user-attachments/assets/28e1eb1c-4ddb-43ae-b193-40332d1b9790)

## 🛠️ System Architecture Diagram

![sysArch](https://github.com/user-attachments/assets/47914c67-714b-479a-8d2c-87875f692f1c)

## Sample of Data
(Symbol 2330 represents [TSMC's](https://www.tsmc.com/english) symbol in the Taiwan stock market.)

1. *Raw Data Received*
    - This represents the raw data I receive from Fugle in real time trading.
    ```json
    {
      "event": "data",
      "data": {
        "symbol": "2330",
        "type": "EQUITY",
        "exchange": "TWSE",
        "market": "TSE",
        "bid": 567,
        "ask": 568,
        "price": 568,
        "size": 4778,
        "volume": 54538,
        "isClose": true,
        "time": 1685338200000000,
        "serial": 6652422
      },
      "id": "<CHANNEL_ID>",
      "channel": "trades"
    }
    ```
2. *Data Aggregated Per Second*
    - The raw data is aggregated into a per-second summary.
    - There could be more than 10 pieces of data received per second from the Fugle WebSocket API.
    ![sec_data_sample](https://github.com/user-attachments/assets/b0569679-530e-4e4c-8b19-e7dd979fd97d)

3. *5-Second Moving Average (MA) Data*
    - Based on the per-second aggregated data, a 5-second moving average (MA) line is further computed.
    ![MA_data_sample](https://github.com/user-attachments/assets/1d2453ae-13b8-4971-a614-a096f645274f)

## 🧰 Tech Stack

- **Containerization**: Docker - managing services across multiple instances
- **Data Pipeline**: Kafka - real-time data streaming
- **Data Aggregation**: Spark - aggregate real-time trading data into moving averages
- **Queue**: asyncio.Queue() - batch processing of data for persisting into storage
- **Database**: MySQL - connection pool, asynchronous
- **Backend Framework**: FastAPI - WebSocket API, asynchronous
- **Frontend**: HTML, CSS, JavaScript
