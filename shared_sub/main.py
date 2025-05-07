import asyncio
import logging
import pulsar

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
    asyncio.create_task(run_consumer())

# Dummy policy_consumer
async def policy_consumer(msg):
    logger.info(f"Processing message: {msg.data().decode()}")
    await asyncio.sleep(1)  # Simulate processing work

async def run_consumer():
    global client
    while True:
        try:
            consumer = client.subscribe(
                pulsar_topic,
                subscription_name='example-policies',
                consumer_type=pulsar.ConsumerType.Shared
            )
            logger.info(f"Subscribed to topic {pulsar_topic}.")

            while True:
                try:
                    msg = await asyncio.to_thread(consumer.receive, 10000)  # timeout 10s
                except pulsar.Timeout:
                    continue  # No message received, loop again

                message_id = msg.message_id()


                try:
                    await policy_consumer(msg)
                    await asyncio.to_thread(consumer.acknowledge, msg)
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