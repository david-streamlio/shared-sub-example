import asyncio
import logging
import string

import pulsar
import random

from fastapi import FastAPI

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pulsar Host
pulsar_host = 'pulsar://localhost:6650'

# Pulsar topic
pulsar_topic = 'public/default/test'

# Global clients
client = pulsar.Client(pulsar_host)

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    logger.info("Starting up")
    # Start 8 concurrent consumer tasks
    for i in range(16):
        asyncio.create_task(run_consumer(i))

    await asyncio.sleep(10)

    # Launch the publisher
    asyncio.create_task(publish_random_messages())

@app.on_event("shutdown")
def shutdown_event():
    logger.info("Shutting down")
    client.close()

# Dummy policy_consumer
async def policy_consumer(consumer_id, msg):
    logger.info(f"Consumer {consumer_id} is processing message: {msg.data().decode()} with message id {msg.message_id()}")
    # await asyncio.sleep(random.uniform(0.0, 3.0))  # Simulate processing work

async def run_consumer(consumer_id):
    global client
    consumer = None
    while True:
        try:
            consumer = client.subscribe(
                pulsar_topic,
                consumer_name=f"consumer-{consumer_id}",
                subscription_name='example-policies',
                consumer_type=pulsar.ConsumerType.Shared,
                receiver_queue_size=1,
                unacked_messages_timeout_ms=30000
            )
            logger.info(f"Subscribed to topic {pulsar_topic}.")

            while True:
                try:
                    msg = await asyncio.to_thread(consumer.receive, 10000)  # timeout 10s
                except pulsar.Timeout:
                    continue  # No message received, loop again

                try:
                    await policy_consumer(consumer_id, msg)
                    await asyncio.to_thread(consumer.acknowledge, msg)
                    logger.info(f"Consumer {consumer_id} acknowledged message: {msg.message_id()}")
                except Exception as e:
                    logger.error(f"Error processing message ID {message_id}: {e}", exc_info=True)
                    await asyncio.to_thread(consumer.negative_acknowledge, msg)

        except (pulsar.Timeout, pulsar.ConnectError, pulsar.LookupError) as e:
            logger.warning(f"Recoverable Pulsar error: {e}. Retrying without client restart...")
            await asyncio.sleep(5)

        except (OSError, ValueError) as e:
            logger.error(f"Unrecoverable Pulsar client error: {e}. Restarting client.")
            client.close()
            await asyncio.sleep(5)
            client = pulsar.Client(pulsar_host)

        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
            client.close()
            await asyncio.sleep(5)
            client = pulsar.Client(pulsar_host)
        finally:
            consumer.close()

def generate_random_string(length=10):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

async def publish_random_messages():
    logger.info("Publisher started")
    global client
    producer = None
    while True:
        try:
            producer = client.create_producer(
                pulsar_topic,
                batching_enabled=True,
                batching_max_messages=10)

            while True:
                msg = generate_random_string()
                producer.send(msg.encode('utf-8'))
                logger.info(f"Published message: {msg}")
                await asyncio.sleep(random.uniform(0.1, 1.5))  # Random interval

        except (pulsar.Timeout, pulsar.ConnectError, pulsar.LookupError) as e:
            logger.warning(f"Recoverable Pulsar error: {e}. Retrying without client restart...")
            await asyncio.sleep(5)
        finally:
            producer.close()