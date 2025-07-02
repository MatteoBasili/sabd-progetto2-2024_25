import csv
import signal
import threading
import logging
import sys
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("csv_writer")

conf = {
    'bootstrap_servers': ['kafka1:29092', 'kafka2:29093'],
    'auto_offset_reset': 'earliest',
    'group_id': 'csv-writer-group',
    'enable_auto_commit': True,
    'value_deserializer': lambda m: m.decode('utf-8')
}

running = True

def shutdown(signum, frame):
    global running
    logger.info("Shutdown signal received")
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def consume_topic(topic, output_file, fieldnames):
    consumer = KafkaConsumer(topic, **conf)
    logger.info(f"Subscribed to topic {topic}")

    with open(output_file, mode='a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csvfile.seek(0, 2)
        if csvfile.tell() == 0:
            writer.writeheader()
            csvfile.flush()
            logger.info(f"Wrote header to {output_file}")

        while running:
            try:
                msg_pack = consumer.poll(timeout_ms=1000)
                if not msg_pack:
                    logger.info("No message received during poll")
                    continue

                for tp, messages in msg_pack.items():
                    for msg in messages:
                        line = msg.value
                        logger.info(f"Received message: {line}")

                        values = line.strip().split(',')
                        if len(values) != len(fieldnames):
                            logger.info(f"Row with wrong number of columns: {line}")
                            continue

                        row = dict(zip(fieldnames, values))
                        writer.writerow(row)
                        csvfile.flush()
            except Exception as e:
                logger.error(f"Exception in consume_topic: {e}")

    consumer.close()
    logger.info(f"Consumer for topic {topic} closed")


if __name__ == "__main__":
    q1_fields = ["batch_id", "print_id", "tile_id", "saturated"]
    q2_fields = ["batch_id", "print_id", "tile_id",
                 "P1", "dP1", "P2", "dP2", "P3", "dP3",
                 "P4", "dP4", "P5", "dP5"]

    t1 = threading.Thread(target=consume_topic, args=("q1-output", "/data/output/q1.csv", q1_fields))
    t2 = threading.Thread(target=consume_topic, args=("q2-output", "/data/output/q2.csv", q2_fields))

    t1.start()
    t2.start()

    t1.join()
    t2.join()
