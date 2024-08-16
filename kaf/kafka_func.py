import asyncio
from aiokafka import AIOKafkaAdminClient, AIOKafkaProducer
from aiokafka.admin import NewTopic
from kafka.errors import KafkaError

async def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1, retention_ms=60000):# 這邊的retention_ms是資料留存時間
    admin_client = AIOKafkaAdminClient(bootstrap_servers='kafka:9092')

    try:
        await admin_client.start()

        topic_metadata = await admin_client.list_topics()
        if topic_name in topic_metadata:
            print(f"Topic '{topic_name}' already exists. Deleting...")
            await admin_client.delete_topics([topic_name])
            print(f"Topic '{topic_name}' deleted. Recreating...")
            
        # ! 先前分區一直是[0]的根本原因是因為，沒有先刪除掉，並且kafka的機制是，沒辦法在建立的topic上改動partitioon
        topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor, config={'retention.ms': str(retention_ms)})

        try:
            await admin_client.create_topics([topic])
            # 下方的好像不必要執行
            print(f"Topic '{topic_name}' created successfully. Got {num_partitions} partitions.")
        except KafkaError as e:
            print(f"Failed to create topic '{topic_name}': {e}")

    except KafkaError as e:
        print(f"Failed to list/delete topics: {e}")
    finally:
        await admin_client.close()