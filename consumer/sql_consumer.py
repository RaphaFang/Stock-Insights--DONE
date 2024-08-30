import os
import aiomysql
import asyncio
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from datetime import datetime
import time

kafka_config = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'MA_kafka_side_group',
    'auto.offset.reset': 'latest',
    'session.timeout.ms': 30000,
    'max.poll.interval.ms': 60000
}

async def build_async_sql_pool():
    return await aiomysql.create_pool(
        host="database-v5.cxu0oc6yqrfs.us-east-1.rds.amazonaws.com",
        user=os.getenv('SQL_USER'),
        password=os.getenv('SQL_PASSWORD'),
        port=3306,
        db='stock_db',
        minsize=1,
        maxsize=10,
    )

async def check_today_table_exists(pool, prefix):
    today = datetime.now().strftime('%Y_%m_%d')
    table_name = f"{prefix}_table_{today}"

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    symbol VARCHAR(10),
                    type VARCHAR(50),
                    MA_type VARCHAR(50),
                    start DATETIME,
                    end DATETIME,
                    current_time DATETIME,
                    first_in_window DATETIME,
                    last_in_window DATETIME,
                    real_data_count INT,
                    filled_data_count INT,
                    sma_5 FLOAT,
                    sum_of_vwap FLOAT,
                    count_of_vwap INT,
                    data_count INT)
            """)
            await conn.commit()
    return table_name

async def batch_insert(pool, table_name, batch_data):
    if not batch_data:
        return

    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            insert_query = f"""
            INSERT INTO {table_name} (
                symbol, type, MA_type, start, end, current_time, 
                first_in_window, last_in_window, real_data_count, 
                filled_data_count, sma_5, sum_of_vwap, count_of_vwap, data_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            await cursor.executemany(insert_query, batch_data)
            await conn.commit()

def create_consumer(kafka_config):
    while True:
        try:
            con = Consumer(kafka_config)
            print("Consumer build up, from consumer_by_partition")
            return con
        except KafkaError as e:
            print(f"Kafka consumer creation failed: {e}. Retrying in 5 seconds...")
            time.sleep(5)

async def mysql_writer(queue, pool, prefix):
    table_name = await check_today_table_exists(pool, prefix)
    batch_data = []

    while True:
        try:
            data = await queue.get()
            batch_data.append(data)
            if len(batch_data) >= 10:  
                await batch_insert(pool, table_name, batch_data)
                batch_data.clear()

            await asyncio.sleep(1)  
        except Exception as e:
            print(f"Error in MySQL writer: {e}")

async def MA_data_consumer(queue, topic, partition=None):
    consumer = create_consumer(kafka_config)
    if partition is not None:
        topic_partition = TopicPartition(topic, partition)
        consumer.assign([topic_partition])
        print(f'Started consuming from topic: {topic}, partition: {partition}.')
    else:
        consumer.subscribe([topic])
        print(f'Started consuming from topic: {topic}, but without partition.')

    try:
        while True:
            msgs = consumer.consume(num_messages=100, timeout=1.0)
            if not msgs:
                continue
            for msg in msgs:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break
                raw = json.loads(msg.value().decode("utf-8"))

                print(f"got MA_data {raw.get('symbol')}")
                await queue.put((
                    raw.get('symbol'), raw.get('type'), raw.get('MA_type'),
                    raw.get('start'), raw.get('end'), raw.get('current_time'),
                    raw.get('first_in_window'), raw.get('last_in_window'),
                    raw.get('real_data_count'), raw.get('filled_data_count'),
                    raw.get('sma_5'), raw.get('sum_of_vwap'),
                    raw.get('count_of_vwap'), raw.get('5_data_count')
                ))

    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Error occurred: {e}, retrying...")
        await asyncio.sleep(5)
    finally:
        consumer.close()
