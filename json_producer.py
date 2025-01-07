from confluent_kafka import Producer
# SerializationContext and MessageField help in defining the context in which the serialization happens.
from confluent_kafka.serialization import SerializationContext, MessageField
# SchemaRegistryClient and JSONSerializer are used to interact with the Schema Registry and serialize messages in JSON format.
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# config and sr_config are configurations for the Kafka producer and Schema Registry, assumed to be defined in a config.py file.
from config import config, sr_config
# time is used for timestamp generation.
import time

# function to convert our Temperature object to a dictionary
class Temperature(object):
    def __init__(self, city, reading, unit, timestamp):
        self.city = city
        self.reading = reading
        self.unit = unit
        self.timestamp = timestamp

# This JSON schema defines the structure of the temperature data
schema_str = """{
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "Temperature",
    "description": "Temperature sensor reading",
    "type": "object",
    "properties": {
      "city": {
        "description": "City name",
        "type": "string"
      },
      "reading": {
        "description": "Current temperature reading",
        "type": "number"
      },
      "unit": {
        "description": "Temperature unit (C/F)",
        "type": "string"
      },
      "timestamp": {
        "description": "Time of reading in ms since epoch",
        "type": "number"
      }
    }
  }"""

# function to convert our Temperature object to a dictionary:
def temp_to_dict(temp, ctx):
    return {"city":temp.city,
            "reading":temp.reading,
            "unit":temp.unit,
            "timestamp":temp.timestamp}

data = [Temperature('London', 12, 'C', round(time.time()*1000)),
        Temperature('Chicago', 63, 'F', round(time.time()*1000)),
        Temperature('Berlin', 14, 'C', round(time.time()*1000)),
        Temperature('Madrid', 18, 'C', round(time.time()*1000)),
        Temperature('Phoenix', 78, 'F', round(time.time()*1000))]

# This callback function reports the success or failure of message delivery.
def delivery_report(err, event):
    if err is not None:
        print(f'Delivery failed on reading for {event.key().decode("utf8")}: {err}')
    else:
        print(f'Temp reading for {event.key().decode("utf8")} produced to {event.topic()}')


if __name__ == '__main__':
    # where messages will be produced.
    topic = 'temp_readings'
    # Creates an instance of the `schema registry client` with the given configuration.
    schema_registry_client = SchemaRegistryClient(sr_config)

    # json_serializer is created using the JSON schema and the serialization function.
    json_serializer = JSONSerializer(schema_str,
                                     schema_registry_client,
                                     temp_to_dict)

    # Creates an instance of the `Producer` with the given configuration.
    producer = Producer(config)

    # For each Temperature object in data, the produce method is called to
    # send the serialized message to Kafka, with city as the key.
    for temp in data:
        producer.produce(topic=topic, key=str(temp.city),
                         value=json_serializer(temp,
                         SerializationContext(topic, MessageField.VALUE)),
                         on_delivery=delivery_report)

    # Waits for all messages in the producer queue to be delivered.
    # This ensures that all messages are sent before the program exits.
    producer.flush()