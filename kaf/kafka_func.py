from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})
    
    try:
        topic_metadata = admin_client.list_topics(timeout=10).topics
        if topic_name in topic_metadata:
            print(f"Topic '{topic_name}' already exists. Deleting...")
            admin_client.delete_topics([topic_name])
            print(f"Topic '{topic_name}' deleted. Recreating...")
    except KafkaException as e:
        print(f"Failed to list/delete topics: {e}")

    # ! 先前分區一直是[0]的根本原因是因為，沒有先刪除掉，並且kafka的機制是，沒辦法在建立的topic上改動partitioon
    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    try:
        fs = admin_client.create_topics([topic])
        ## 下方的好像不必要執行
        # for topic, future in fs.items():
        #     try:
        #         future.result()
        #         print(f"Topic '{topic}' created successfully. Got {num_partitions} partitions.")
        #     except KafkaException as e:
        #         print(f"Failed to create topic '{topic}': {e}")
    except KafkaException as e:
        print(f"Failed to create topic '{topic_name}': {e}")