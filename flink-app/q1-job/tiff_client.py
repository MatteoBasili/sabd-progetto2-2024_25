# tiff_client.py
import requests
import umsgpack
import queue
import threading
import time

class ChallengerStream(threading.Thread):
    def __init__(self, endpoint, bench_id, out_queue, poll_interval=0.1):
        super().__init__()
        self.endpoint = endpoint
        self.bench_id = bench_id
        self.queue = out_queue
        self.stop_flag = False
        self.poll_interval = poll_interval
        self.session = requests.Session()

    def run(self):
        i = 0
        while not self.stop_flag:
            try:
                url = f"{self.endpoint}/api/next_batch/{self.bench_id}"
                resp = self.session.get(url)
                if resp.status_code == 404:
                    print("No more batches.")
                    break
                batch = umsgpack.unpackb(resp.content)
                self.queue.put(batch)
                i += 1
                time.sleep(self.poll_interval)
            except Exception as e:
                print(f"Error fetching batch: {e}")
                break

    def stop(self):
        self.stop_flag = True

