import argparse
import os
import json
import time
import base64
import logging
import requests
import umsgpack
import csv
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("l-pbf_client")

KAFKA_BROKERS = ["kafka1:29092", "kafka2:29093"]
TOPIC_INPUT = "tiff-batches"
TOPIC_OUTPUT = "l-pbf-output"
GROUP_ID = "result-consumer"


def positive_int(value: str) -> int:
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return ivalue

def wait_for_kafka_ready():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            logger.info("✅ Kafka is ready.")
            return producer
        except KafkaError as e:
            logger.warning(f"⏳ Kafka not ready: {e}")
            time.sleep(2)

def wait_for_challenger_ready(url):
    while True:
        try:
            r = requests.get(f"{url}/api/history")
            if r.status_code == 200:
                logger.info("✅ Local Challenger is up.")
                break
        except Exception as e:
            logger.warning(f"⏳ Waiting for Local Challenger: {e}")
        time.sleep(2)

def initialize_benchmark(session, endpoint, limit=None):
    payload = {
        "apitoken": "uniroma2-dicii-mbat",
        "name": "sabd_project",
        "test": True,
    }
    if limit:
        payload["max_batches"] = limit

    create_resp = session.post(f"{endpoint}/api/create", json=payload)
    create_resp.raise_for_status()
    bench_id = create_resp.json()
    logger.info(f"🆕 Created benchmark with ID: {bench_id}")

    start_resp = session.post(f"{endpoint}/api/start/{bench_id}")
    start_resp.raise_for_status()
    logger.info(f"🏁 Started benchmark {bench_id}")
    return bench_id

def produce_batches(session, endpoint, bench_id, producer, limit=None):
    produced = 0
    while not limit or produced < limit:
        logger.info(f"📦 Requesting batch {produced}")
        resp = session.get(f"{endpoint}/api/next_batch/{bench_id}")
        if resp.status_code == 404:
            logger.info("🛑 No more batches available.")
            break
        elif resp.status_code != 200:
            logger.warning(f"⚠️ Error fetching batch {produced}: {resp.text}")
            time.sleep(1)
            continue

        batch = umsgpack.unpackb(resp.content)
        # Base64 encoding to make it JSON-safe
        batch["tif"] = base64.b64encode(batch["tif"]).decode("utf-8")
        partition_key = f"{batch['print_id']}-{batch['tile_id']}".encode("utf-8")
        producer.send(TOPIC_INPUT, key=partition_key, value=batch)
        producer.flush()
        logger.info(f"📤 Sent batch {produced} (layer {batch.get('layer')}) to Kafka")
        produced += 1
        time.sleep(0.1)
        
    logger.info(f"📦 Total batches produced: {produced}")
    return produced

def consume_and_post_results(endpoint, bench_id, expected_results):
    session = requests.Session()
    consumer = KafkaConsumer(
        TOPIC_OUTPUT,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: m.decode("utf-8")   # CSV row
    )

    logger.info("🕓 Waiting for results from Flink...")
    received = 0
    
    while received < expected_results:
        msg_pack = consumer.poll(timeout_ms=1000)  # wait max 1s for a message

        if not msg_pack:
            continue

        for tp, messages in msg_pack.items():
            for msg in messages:
                csv_line = msg.value.rstrip("\n")

                try:
                    batch_id, print_id, tile_id, saturated, centroids_json = next(
                        csv.reader([csv_line])
                    )
                    centroids = json.loads(centroids_json)
                except Exception as e:
                    logger.error(f"⚠️  Bad message: {e} | {csv_line}")
                    continue

                result = {
                    "batch_id": int(batch_id),
                    "query": 0,
                    "print_id": print_id,
                    "tile_id": int(tile_id),
                    "saturated": int(saturated),
                    "centroids": centroids
                }

                logger.info(f"✅ Received result for batch {batch_id}")

                try:
                    packed = umsgpack.packb(result)
                    url = f"{endpoint}/api/result/0/{bench_id}/{batch_id}"
                    session.post(url, data=packed).raise_for_status()
                    logger.info(f"📬 Posted result for batch {batch_id}")
                except Exception as e:
                    logger.error(f"❌ Failed to post result: {e}")
            
                received += 1
        
        
    consumer.close()
    logger.info(f"📦 Results posted: {received}")

def main():
    parser = argparse.ArgumentParser(description="Kafka client for DEBS GC")
    parser.add_argument("--limit", type=positive_int,
                        help="Maximum number of batches to process")
    args = parser.parse_args()

    limit            = args.limit

    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")

    wait_for_challenger_ready(endpoint)
    producer = wait_for_kafka_ready()
    session = requests.Session()

    # Step 1: Initialize benchmark
    bench_id = initialize_benchmark(session, endpoint, limit=limit)

    # Step 2: Produce batches to Kafka
    produced_batches = produce_batches(session, endpoint, bench_id, producer, limit=limit)

    expected_results = produced_batches

    # Step 3: Consume results from Kafka and send them to local-challenger
    consume_and_post_results(endpoint, bench_id, expected_results=expected_results)

    # Step 4: End benchmark
    end_resp = session.post(f"{endpoint}/api/end/{bench_id}")
    end_resp.raise_for_status()
    logger.info(f"✅ Benchmark {bench_id} completed.")
    print(f"Result: {end_resp.text}")

if __name__ == "__main__":
    main()

