from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from config import config


def set_consumer_configs():
    config['group.id'] = 'temp_group'
    config['auto.offset.reset'] = 'earliest'

class Temperature(object):
    def __init__(self, city, reading, unit, timestamp):
        self.city = city
        self.reading = reading
        self.unit = unit
        self.timestamp = timestamp
