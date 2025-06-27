# q1_job.py
import socket
import time
import requests
import os
import io
import numpy as np
from PIL import Image
import json
import base64

from pyflink.common import Configuration, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, Schema

from tiff_client import ChallengerStream


def count_saturated_pixels(tiff_bytes, low_thresh=5000, high_thresh=65000):
    img = Image.open(io.BytesIO(tiff_bytes))
    arr = np.array(img)
    
    valid_pixels = arr[arr >= low_thresh]

    saturated = np.sum(valid_pixels > high_thresh)
    return saturated

def wait_for_kafka(host='kafka', port=9092):
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("✅ Kafka is up.")
                break
        except OSError:
            print("⏳ Waiting for Kafka...")
            time.sleep(2)

def wait_for_local_challenger(url):
    while True:
        try:
            r = requests.get(f"{url}/api/history")
            if r.status_code == 200:
                print("✅ Local Challenger is up.")
                break
        except requests.exceptions.ConnectionError:
            pass

        print("⏳ Waiting for Local Challenger...")
        time.sleep(2)

def main():
    # Wait for local challenger
    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")  # adapt if needed
    wait_for_local_challenger(endpoint)
    
    # Wait for Kafka
    wait_for_kafka()
    
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
    stream_thread = ChallengerStream(endpoint, bench_id, topic="tiff-batches")
    stream_thread.start()

    # 4. Flink job
    config = Configuration()
    config.set_string("pipeline.jars",
                      "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)
    
    # Create Table Environment
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("tiff-batches") \
        .set_group_id("flink-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

    parsed = ds.map(lambda raw: (
        lambda data: Row(
            batch_id=int(data["batch_id"]),
            print_id=str(data["print_id"]),
            tile_id=int(data["tile_id"]),
            saturated=count_saturated_pixels(base64.b64decode(data["tif"]))
        )
    )(json.loads(raw)), output_type=Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id", "saturated"],
        [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
    ))

    csv_strings = parsed.map(lambda t: ",".join(map(str, t)), output_type=Types.STRING())
    
    kafka_sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_value_serialization_schema(SimpleStringSchema())
        .set_topic("q1-output")
        .build()
    ) \
    .build()

    csv_strings.sink_to(kafka_sink)
    
    # Convert to table
    t_env.create_temporary_view(
    "parsed_view",
    parsed,
    Schema.new_builder()
        .column("batch_id", DataTypes.INT())
        .column("print_id", DataTypes.STRING())
        .column("tile_id", DataTypes.INT())
        .column("saturated", DataTypes.INT())
        .build()
)
    
    # File sink (filesystem + csv)
    t_env.execute_sql("""
        CREATE TABLE file_sink (
            batch_id INT,
            print_id STRING,
            tile_id INT,
            saturated INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///data/output/q1.csv',
            'format' = 'csv'
        )
    """)

    # Write to file
    t_env.execute_sql("INSERT INTO file_sink SELECT * FROM parsed_view")

    env.execute("Q1 Saturated Pixel Counter")

    # End bench (optional cleanup)
    # Nota: queste righe non verranno mai eseguite perché `env.execute()` è bloccante
    # session.post(f"{endpoint}/api/end/{bench_id}")
    # stream_thread.stop()

if __name__ == "__main__":
    main()

