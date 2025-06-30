import requests
import umsgpack
import socket
import time
import json
import base64
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os

## TODO:
#  riaggiustare il codice affiché spari batches su kafka indefinitamente!
#  (rimettere il "while True:")


def wait_for_kafka_ready(topic="tiff-batches"):
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=["kafka1:9092", "kafka2:9092"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print(f"✅ Kafka is ready.")
            return producer
        except KafkaError as e:
            print(f"⏳ Kafka not ready: Waiting for Kafka... ({e})")
        time.sleep(2)


def wait_for_local_challenger(url):
    while True:
        try:
            r = requests.get(url)
            if r.status_code == 200:
                print(f"✅ Local Challenger is up.")
                break
        except:
            pass
        print(f"⏳ Waiting for Local Challenger...")
        time.sleep(2)


def main():
    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")
    wait_for_local_challenger(f"{endpoint}/api/history")
    
    producer = wait_for_kafka_ready()

    session = requests.Session()

    # Create and start benchmark
    resp = session.post(f"{endpoint}/api/create", json={
        "apitoken": "polimi-deib",
        "name": "sabd_project",
        "test": True
    })
    bench_id = resp.json()
    session.post(f"{endpoint}/api/start/{bench_id}")

    i = 0
    # Riaggiustare questo!!!
    while (i < 100):
    #while True:
        try:
            url = f"{endpoint}/api/next_batch/{bench_id}"
            resp = session.get(url)
            if resp.status_code == 404:
                print("❌ No more batches.")
                break
            batch = umsgpack.unpackb(resp.content)
            batch["tif"] = base64.b64encode(batch["tif"]).decode("utf-8")
            producer.send("tiff-batches", value=batch)
            producer.flush()
            print(f"📤 Sent batch {i} to Kafka")
            i += 1
            time.sleep(0.1)
        except Exception as e:
            print(f"⚠️ Error: {e}")
            break

if __name__ == "__main__":
    main()

