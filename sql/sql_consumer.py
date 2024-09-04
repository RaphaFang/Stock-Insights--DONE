import os
import aiomysql
import json
from datetime import datetime
import time

import asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


async def build_async_sql_pool():
    return await aiomysql.create_pool(
        host="database-v5.cxu0oc6yqrfs.us-east-1.rds.amazonaws.com",
        user=os.getenv('SQL_USER'),
        password=os.getenv('SQL_PASSWORD'),
        # ----------------------------------------------------------------
        # host="localhost",
        # user=os.getenv('SQL_USER_LOCAL'),
        # password=os.getenv('SQL_PASSWORD_LOCAL'),
        # ----------------------------------------------------------------
        port=3306,
        db='stock_db',
        minsize=1,
        maxsize=10,
    )
    # try:
    #     pool = await aiomysql.create_pool(
    #         host='database-v5.cxu0oc6yqrfs.us-east-1.rds.amazonaws.com',
    #         # port=3306,
    #         user=os.getenv('SQL_USER'),
    #         password=os.getenv('SQL_PASSWORD'),
    #         db='stock_db',
    #     )
    #     print("成功連接到 RDS！")
    #     pool.close()
    #     await pool.wait_closed()
    # except Exception as e:
    #     print(f"連接失敗，錯誤訊息: {e}")

async def check_today_table_exists(pool, prefix):
    today = datetime.now().strftime('%Y_%m_%d')
    table_name = f"{prefix}_table_{today}"
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            if prefix == "MA":
                await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{table_name}` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `symbol` VARCHAR(10),
                        `type` VARCHAR(50),
                        `MA_type` VARCHAR(50),
                        `start` DATETIME,
                        `end` DATETIME,
                        `current_time` DATETIME,
                        `first_in_window` DATETIME,
                        `last_in_window` DATETIME,
                        `real_data_count` INT,
                        `filled_data_count` INT,
                        `sma_5` FLOAT,
                        `sum_of_vwap` FLOAT,
                        `count_of_vwap` INT,
                        `data_count` INT
                    )
                """)
            elif prefix == "sec":
                await cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{table_name}` (
                        `id` INT AUTO_INCREMENT PRIMARY KEY,
                        `symbol` VARCHAR(10),
                        `type` VARCHAR(50),
                        `start` DATETIME,
                        `end` DATETIME,
                        `current_time` DATETIME,
                        `last_data_time` DATETIME,
                        `real_data_count` INT,
                        `filled_data_count` INT,
                        `real_or_filled` VARCHAR(10),
                        `vwap_price_per_sec` FLOAT,
                        `size_per_sec` INT,
                        `volume_till_now` INT,
                        `yesterday_price` FLOAT,
                        `price_change_percentage` FLOAT
                    )
                """)
            await conn.commit()
    return table_name


async def create_consumer(topic):
    while True:
        try:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers='10.0.1.138:9092',
                group_id=f'{topic}_to_sql_group',
                auto_offset_reset='latest',
                session_timeout_ms=30000,
                max_poll_interval_ms=60000,
                # heartbeat_interval_ms=3000,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            await consumer.start()
            return consumer
        
        except KafkaError as e:
            logging.info(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

async def consumer_to_queue(prefix, queue, topic):
    try:
        if prefix=="sec":
            consumer = await create_consumer("kafka_per_sec_data")
        elif prefix=="MA":
            consumer = await create_consumer("kafka_MA_data")
    #         try:
    #     async for message in consumer:
    #         await MA_queue.put(message.value.decode('utf-8'))
    # finally:
    #     await consumer.stop()

        async for message in consumer:
            raw = json.loads(message.value().decode("utf-8"))  # Parse string into a dictionary
            logging.info(type(raw))
            logging.info(raw)
            logging.info(f"from data_to_sql_consumer\ngot {topic}: {raw.get('symbol')}")

            if isinstance(raw, dict):
                await queue.put((
                        raw.get('symbol'), raw.get('type'), raw.get('MA_type'),
                        raw.get('start'), raw.get('end'), raw.get('current_time'),
                        raw.get('first_in_window'), raw.get('last_in_window'),
                        raw.get('real_data_count'), raw.get('filled_data_count'),
                        raw.get('sma_5'), raw.get('sum_of_vwap'),
                        raw.get('count_of_vwap'), raw.get('5_data_count')
                    ))
            else:
                logging.error("期望獲取字典類型數據，但實際獲得了其他類型。跳過此消息。")

    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    except Exception as e:
        logging.info(f"Error occurred consumer_to_queue: {e}, retrying...")
        await asyncio.sleep(5)
    finally:
        await consumer.stop()

async def batch_insert(prefix, pool, table_name, batch_data):
    if not batch_data:
        return

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            if prefix == "MA":
                insert_query = f"""
                INSERT INTO {table_name} (
                    symbol, type, MA_type, start, end, current_time, 
                    first_in_window, last_in_window, real_data_count, 
                    filled_data_count, sma_5, sum_of_vwap, count_of_vwap, data_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            elif prefix == "sec":
                insert_query = f"""
                INSERT INTO {table_name} (
                    symbol, type, start, end, current_time, last_data_time,
                    real_data_count, filled_data_count, real_or_filled,
                    vwap_price_per_sec, size_per_sec, volume_till_now,
                    yesterday_price, price_change_percentage
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
            await cursor.executemany(insert_query, batch_data)
            await conn.commit()

async def queue_to_mysql(prefix, queue, pool):
    table_name = await check_today_table_exists(pool, prefix)
    batch_data = []

    try:
        while True:
            while not queue.empty():
                data = await queue.get()
                batch_data.append(data)
                if len(batch_data) >= 10:  
                    await batch_insert(prefix, pool, table_name, batch_data)
                    batch_data.clear()
            await asyncio.sleep(1)  

    except Exception as e:
        logging.error(f"Error in MySQL writer: {e}")