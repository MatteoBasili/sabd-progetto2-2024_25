from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import time, logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("topic_creator")

BOOTSTRAP_SERVERS = ["kafka1:29092", "kafka2:29093"]


def main():
    topics_cfg = [
        # ---------------  INPUT  -----------------
        {"name": "tiff-batches", "partitions": 1, "replication": 2},
        # --------------- OUTPUTS -----------------
        {"name": "q1-output",    "partitions": 1, "replication": 2},
        {"name": "q2-output",    "partitions": 1, "replication": 2},
        {"name": "l-pbf-output", "partitions": 1, "replication": 2},
    ]

    # Retry loop: wait until at least one broker is ready
    while True:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="topic_creator"
            )
            break
        except NoBrokersAvailable:
            log.info("⏳ Kafka not ready yet, retrying…")
            time.sleep(2)

    new_topics = [
        NewTopic(t["name"], t["partitions"], t["replication"])
        for t in topics_cfg
    ]

    try:
        admin.create_topics(new_topics=new_topics, validate_only=False)
        log.info("✅ Topics created.")
    except TopicAlreadyExistsError:
        log.warning("⚠️  Some topics already exist; leaving them unchanged.")
    finally:
        admin.close()

if __name__ == "__main__":
    main()

