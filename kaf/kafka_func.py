from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

def create_kafka_topic(topic_name, num_partitions=1, replication_factor=1):
    admin_client = AdminClient({'bootstrap.servers': 'kafka:9092'})
    
    topic = NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
    
    try:
        admin_client.create_topics([topic])
        print(f"Topic '{topic_name}' created successfully.")
    except KafkaException as e:
        if "TopicExistsError" in str(e):
            print(f"Topic '{topic_name}' already exists.")
        else:
            print(f"Failed to create topic '{topic_name}': {e}")