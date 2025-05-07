from multiprocessing import Process

import logging
import pulsar
import random
import string
import time
import os

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pulsar Host
pulsar_host = 'pulsar://localhost:6650'

# Pulsar topic
pulsar_topic = 'public/default/test'

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def run_producer():
    client = pulsar.Client(pulsar_host)
    producer = client.create_producer(
        pulsar_topic,
        batching_enabled=True,
        batching_max_messages=1000)

    try:
        while True:
            msg = generate_random_string()
            producer.send(msg.encode('utf-8'))
            # logger.info(f"Published message: {msg}")
            # time.sleep(random.uniform(0.1, 1.5))  # Random interval
    except KeyboardInterrupt:
        print(f"Producer Stopping...")
    finally:
        producer.close()
        client.close()

def run_consumer(name: str):
    client = pulsar.Client(pulsar_host)
    consumer = client.subscribe(
                pulsar_topic,
                consumer_name=f"consumer-{name}",
                subscription_name='example-policies',
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=1,
                unacked_messages_timeout_ms=30000
            )

    logger.info(f"Subscribed to topic {pulsar_topic}.")

    logger.info(f"[{name}] started on PID {os.getpid()}")

    try:
        while True:
            msg = consumer.receive()
            logger.info(f"Consumer {name} is processing message: {msg.data().decode()} with message id {msg.message_id()}")
            time.sleep(random.uniform(0.1, 1.5))  # Random interval
            consumer.acknowledge(msg)
    except KeyboardInterrupt:
        print(f"[{name}] Stopping...")
    finally:
        consumer.close()
        client.close()

if __name__ == "__main__":
    import os

    num_producers = 1
    num_consumers = 16
    processes = []

    for i in range(num_producers):
        p = Process(target=run_producer)
        p.start()
        processes.append(p)

    for i in range(num_consumers):
        p = Process(target=run_consumer, args=(f"consumer-{i}",))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        for p in processes:
            p.terminate()
