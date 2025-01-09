from confluent_kafka.admin import (AdminClient, NewTopic, ConfigResource)
from config import config

def topic_exists(admin, topic):
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False