from confluent_kafka.admin import (AdminClient, NewTopic, ConfigResource)
from config import config

def topic_exists(admin, topic):
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False

def create_topic(admin, topic):
    new_topic = NewTopic(topic, num_partitions=6, replication_factor=3)
    result_dict = admin.create_topics([new_topic])
    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            print("Topic {} created".format(topic))
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

def get_max_size(admin, topic):
    resource = ConfigResource('topic', topic)
    result_dict = admin.describe_configs([resource])
    config_entries = result_dict[resource].result()
    max_size = config_entries['max.message.bytes']
    return max_size.value