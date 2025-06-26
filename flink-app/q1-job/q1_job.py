# q1_job.py
import time
import requests
import os
import io
import numpy as np
from PIL import Image
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import SourceFunction, SinkFunction
from pyflink.common.typeinfo import Types
import queue
from tiff_client import ChallengerStream

class TiffSource(SourceFunction):
    def __init__(self, data_queue):
        self.data_queue = data_queue
        self.running = True

    def run(self, ctx):
        while self.running:
            try:
                batch = self.data_queue.get(timeout=1)
                ctx.collect(batch)
            except queue.Empty:
                continue

    def cancel(self):
        self.running = False

class CsvSink(SinkFunction):
    def __init__(self, file_path):
        self.file_path = file_path
        self.file = None

    def open(self, runtime_context):
        self.file = open(self.file_path, "a")  # append mode

    def invoke(self, value, context):
        self.file.write(",".join(map(str, value)) + "\n")
        self.file.flush()

    def close(self):
        if self.file:
            self.file.close()

def count_saturated_pixels(tiff_bytes, low_thresh=5000, high_thresh=65000):
    img = Image.open(io.BytesIO(tiff_bytes))
    arr = np.array(img)
    saturated = np.sum((arr > high_thresh))
    return saturated

def wait_for_local_challenger(url, timeout=60):
    start = time.time()
    while True:
        try:
            r = requests.get(f"{url}/api/history")
            if r.status_code == 200:
                print("✅ Local Challenger is up.")
                break
        except requests.exceptions.ConnectionError:
            pass

        if time.time() - start > timeout:
            raise RuntimeError("❌ Timeout waiting for Local Challenger")
        print("⏳ Waiting for Local Challenger...")
        time.sleep(2)

def main():
    # Start REST client thread
    queue_batches = queue.Queue()
    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")  # adapt if needed
    wait_for_local_challenger(endpoint)
    session = requests.Session()

    # 1. Create bench
    resp = session.post(f"{endpoint}/api/create", json={
        "apitoken": "polimi-deib",
        "name": "q1_job",
        "test": True
    })
    bench_id = resp.json()

    # 2. Start it
    session.post(f"{endpoint}/api/start/{bench_id}")

    # 3. Start polling thread
    stream_thread = ChallengerStream(endpoint, bench_id, queue_batches)
    stream_thread.start()

    # 4. Flink job
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)

    ds = env.add_source(TiffSource(queue_batches))

    parsed = ds.map(lambda batch: (
        batch["seq_id"],
        batch["print_id"],
        batch["tile_id"],
        count_saturated_pixels(batch["tif"])
    ), output_type=Types.TUPLE([Types.INT(), Types.STRING(), Types.INT(), Types.INT()]))

    parsed.add_sink(CsvSink("/data/output/q1.csv"))

    env.execute("Q1 Saturated Pixel Counter")

    # 5. End bench (optional cleanup)
    session.post(f"{endpoint}/api/end/{bench_id}")
    stream_thread.stop()

if __name__ == "__main__":
    main()

