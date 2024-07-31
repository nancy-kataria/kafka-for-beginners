# Imports the `Producer` class from the `confluent_kafka` library, which is used to produce messages to a Kafka cluster
from confluent_kafka import Producer
# Imports a `config` object or dictionary from a `config` module. This `config` will
# contain configuration settings for the Kafka producer, such as bootstrap servers, security settings, etc.
from config import config

# This function is a callback that will be called when the producer
# request is completed, either successfully or with an error.
def callback(err, event):
    # `event`: The event object which contains metadata about the produced message
    # The error message, if any.
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        # If there is no error, it decodes the message value from bytes to a UTF-8
        # string and prints a success message indicating to which partition the message was sent.
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

# This function produces a message to the `hello_topic` topic
def say_hello(producer, key):
    # `key`: The key for the message, which is used to determine the partitioning
    value = f'Hello {key}!'
    # Sends the message to Kafka. It includes the topic (`hello_topic`),
    # the message value, the message key, and specifies the `callback` function to be called upon completion.
    producer.produce('hello_topic', value, key, on_delivery=callback)

# Ensures that this block only runs if the script is executed directly, not when imported as a module.
if __name__ == '__main__':
    # Creates an instance of the `Producer` with the given configuration.
    producer = Producer(config)
    keys = ['Brandon', 'Cinthia', 'Dan', 'Ema', 'Fargo', 'Argus']
    # Iterates over the list of keys and calls the `say_hello` function for each key, producing a message for each name.
    [say_hello(producer, key) for key in keys]
    # Waits for all messages in the producer queue to be delivered.
    # This ensures that all messages are sent before the program exits.
    producer.flush()