# tiff_client.py
import requests
import umsgpack
import threading
import time
import json
import base64
from kafka import KafkaProducer

class ChallengerStream(threading.Thread):
    def __init__(self, endpoint, bench_id, topic="tiff-batches", poll_interval=0.1):
        super().__init__()
        self.endpoint = endpoint
        self.bench_id = bench_id
        self.topic = topic
        self.stop_flag = False
        self.poll_interval = poll_interval
        self.session = requests.Session()

        self.producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def run(self):
        i = 0
        while not self.stop_flag:
            try:
                url = f"{self.endpoint}/api/next_batch/{self.bench_id}"
                resp = self.session.get(url)
                if resp.status_code == 404:
                    print("❌ No more batches.")
                    break
                batch = umsgpack.unpackb(resp.content)

                # Encode bytes to base64 string (do NOT modify other fields)
                batch["tif"] = base64.b64encode(batch["tif"]).decode("utf-8")

                # Send to Kafka topic "tiff-batches"
                self.producer.send(self.topic, value=batch)
                self.producer.flush()
                print(f"📤 Sent batch {i} to Kafka")
                i += 1
                time.sleep(self.poll_interval)
            except Exception as e:
                print(f"⚠️ Error fetching/sending batch: {e}")
                break

    def stop(self):
        self.stop_flag = True

