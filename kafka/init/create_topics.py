from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import time

BOOTSTRAP_SERVERS = ["kafka1:9092", "kafka2:9092"]

TOPICS_TO_CREATE = [
    {
        "name": "tiff-batches",
        "partitions": 1,
        "replication_factor": 1
    },
    {
        "name": "q1-output",
        "partitions": 1,
        "replication_factor": 1
    },
    {
        "name": "filtered-batches",
        "partitions": 1,
        "replication_factor": 1
    },
    {
        "name": "q2-output",
        "partitions": 1,
        "replication_factor": 1
    },
    {
        "name": "outlier-batches",
        "partitions": 1,
        "replication_factor": 1
    }
]


def create_kafka_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="topic_creator")

        topic_list = []
        for t in TOPICS_TO_CREATE:
            topic = NewTopic(name=t["name"], num_partitions=t["partitions"], replication_factor=t["replication_factor"])
            topic_list.append(topic)

        admin.create_topics(new_topics=topic_list, validate_only=False)
        print("✅ Topics created successfully.")

    except TopicAlreadyExistsError as e:
        print(f"⚠️ Some topics already exist: {e}")
    except NoBrokersAvailable:
        print("❌ Kafka brokers not available. Retrying in 5 seconds...")
        time.sleep(5)
        create_kafka_topics()
    except Exception as e:
        print(f"❌ Error creating topics: {e}")
    finally:
        try:
            admin.close()
        except:
            pass


if __name__ == "__main__":
    create_kafka_topics()

