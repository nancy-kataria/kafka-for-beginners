from confluent_kafka import Consumer, KafkaException
from config import config

def set_consumer_configs():
    config['group.id'] = 'hello_group'
    # so that we can consume events that are already in our target topic
    config['auto.offset.reset'] = 'earliest'
    # so that we can control the committing of offsets for our consumer.
    config['enable.auto.commit'] = False

def assignment_callback(consumer, partitions):
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')

# Ensures that this block only runs if the script is executed directly, not when imported as a module.
if __name__ == '__main__':
    # Calls the function to set the consumer configurations.
    set_consumer_configs()
    # Creates an instance of the Kafka `Consumer` with the given configuration
    consumer = Consumer(config)
    # Subscribes the consumer to the `hello_topic` topic and sets the assignment callback.
    consumer.subscribe(['hello_topic'], on_assign=assignment_callback)

    # We’re going to be adding an endless loop in the next step, so we’ll use the except KeyboardInterrupt
    # to catch a CTRL-C to stop the program, and then we’ll call consumer.close() in the finally block to
    # make sure we clean up after ourselves.
    try:
        # Enters a loop to continuously poll for new messages
        while True:
            # Polls the Kafka broker for new messages, waiting up to 1 second.
            event = consumer.poll(1.0)
            # If no message is received, continues to the next iteration of the loop
            if event is None:
                continue
            # Checks if the event contains an error. If so, raises a KafkaException
            if event.error():
                raise KafkaException(event.error())
            else:
                # Decodes the message value from bytes to a UTF-8 string
                val = event.value().decode('utf8')
                # Retrieves the partition number of the message
                partition = event.partition()
                print(f'Received: {val} from partition {partition}')
                # Commits the offset of the consumed message manually
                consumer.commit(event)
    # Catches a keyboard interrupt (Ctrl+C) and prints a cancellation message
    except KeyboardInterrupt:
        print('Canceled by user.')
    finally:
        # Ensures that the consumer is closed properly
        consumer.close()