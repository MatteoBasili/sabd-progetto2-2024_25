import requests
import umsgpack
import time
import json
import base64
import os
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("batch_producer")


def wait_for_kafka_ready():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka1:29092", "kafka2:29093"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info("✅ Kafka is ready.")
            return producer
        except KafkaError as e:
            logger.warning(f"⏳ Kafka not ready: {e}")
            time.sleep(2)


def wait_for_local_challenger(url):
    while True:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                logger.info("✅ Local Challenger is up.")
                break
        except requests.exceptions.RequestException as e:
            logger.warning(f"⏳ Waiting for Local Challenger... {e}")
        time.sleep(2)


def main():
    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")
    wait_for_local_challenger(f"{endpoint}/api/history")
    
    producer = wait_for_kafka_ready()

    session = requests.Session()

    try:
        # Create and start benchmark
        resp = session.post(f"{endpoint}/api/create", json={
            "apitoken": "polimi-deib",
            "name": "sabd_project",
            "test": True
        })
        resp.raise_for_status()
        bench_id = resp.json()
        session.post(f"{endpoint}/api/start/{bench_id}")
        logger.info(f"🏁 Started benchmark with ID: {bench_id}")
    except Exception as e:
        logger.error(f"❌ Failed to initialize benchmark: {e}")
        return

    i = 0
    while (i < 90):
    #while True:
        try:
            url = f"{endpoint}/api/next_batch/{bench_id}"
            resp = session.get(url)

            if resp.status_code == 404:
                logger.info("❌ No more batches available.")
                break
            elif resp.status_code != 200:
                logger.warning(f"⚠️ Unexpected response {resp.status_code}: {resp.text}")
                time.sleep(1)
                continue

            batch = umsgpack.unpackb(resp.content)
            batch["tif"] = base64.b64encode(batch["tif"]).decode("utf-8")
            producer.send("tiff-batches", value=batch)
            producer.flush()
            logger.info(f"📤 Sent batch {i} (layer {batch.get('layer')}) to Kafka")
            i += 1
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Error while processing batch {i}: {e}")
            break


if __name__ == "__main__":
    main()

