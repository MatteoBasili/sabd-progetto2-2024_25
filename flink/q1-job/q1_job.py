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

def filter_tile_pixels(tiff_bytes, low_thresh=5000, high_thresh=65000):
    """
    Filter the TIFF tile pixels: set pixels < low_thresh or > high_thresh to zero.
    Return the filtered TIFF bytes.
    """
    img = Image.open(io.BytesIO(tiff_bytes))
    arr = np.array(img)

    # Mask pixels outside [low_thresh, high_thresh] range by setting to zero
    mask = (arr >= low_thresh) & (arr <= high_thresh)
    filtered_arr = np.where(mask, arr, 0).astype(np.uint16)

    # Save back to TIFF bytes in memory
    filtered_img = Image.fromarray(filtered_arr)
    with io.BytesIO() as output:
        filtered_img.save(output, format="TIFF")
        filtered_bytes = output.getvalue()

    return filtered_bytes


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
    endpoint = os.getenv("LOCAL_CHALLENGER_URL", "http://local-challenger:8866")
    wait_for_local_challenger(endpoint)
    wait_for_kafka()

    session = requests.Session()

    # Create benchmark
    resp = session.post(f"{endpoint}/api/create", json={
        "apitoken": "polimi-deib",
        "name": "q1_job",
        "test": True
    })
    bench_id = resp.json()

    # Start benchmark
    session.post(f"{endpoint}/api/start/{bench_id}")

    # Start polling thread for fetching batches and sending raw to kafka
    stream_thread = ChallengerStream(endpoint, bench_id, topic="tiff-batches")
    stream_thread.start()

    config = Configuration()
    config.set_string("pipeline.jars",
                      "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka source from raw batches topic
    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("tiff-batches") \
        .set_group_id("flink-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

    def process_batch(raw):
        data = json.loads(raw)

        # Count saturated pixels
        tif_bytes = base64.b64decode(data["tif"])
        saturated = count_saturated_pixels(tif_bytes)

        # Filter tile pixels (for Q2)
        filtered_bytes = filter_tile_pixels(tif_bytes)
        filtered_b64 = base64.b64encode(filtered_bytes).decode("utf-8")

        # Prepare output row for q1-output topic and file sink
        q1_row = Row(
            batch_id=int(data["batch_id"]),
            print_id=str(data["print_id"]),
            tile_id=int(data["tile_id"]),
            saturated=saturated
        )

        # Prepare filtered batch dict for filtered-batches topic
        filtered_batch = {
            "batch_id": data["batch_id"],
            "print_id": data["print_id"],
            "tile_id": data["tile_id"],
            "layer": data["layer"],
            "tif": filtered_b64
        }

        return q1_row, filtered_batch

    processed = ds.map(
        lambda raw: process_batch(raw),
        output_type=Types.TUPLE([
            Types.ROW_NAMED(
                ["batch_id", "print_id", "tile_id", "saturated"],
                [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
            ),
            Types.MAP(Types.STRING(), Types.STRING())
        ])
    )

    # Split streams: one for Q1 output, one for filtered batches
    q1_output = processed.map(lambda x: x[0], output_type=Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id", "saturated"],
        [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
    ))

    filtered_batches = processed.map(lambda x: x[1], output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Sink for Q1 output - Kafka topic "q1-output"
    q1_csv_strings = q1_output.map(lambda t: ",".join(map(str, t)), output_type=Types.STRING())
    kafka_sink_q1 = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_value_serialization_schema(SimpleStringSchema())
            .set_topic("q1-output")
            .build()
        ) \
        .build()
    q1_csv_strings.sink_to(kafka_sink_q1)

    # File sink for Q1 output
    t_env.create_temporary_view(
        "q1_view",
        q1_output,
        Schema.new_builder()
        .column("batch_id", DataTypes.INT())
        .column("print_id", DataTypes.STRING())
        .column("tile_id", DataTypes.INT())
        .column("saturated", DataTypes.INT())
        .build()
    )

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

    t_env.execute_sql("INSERT INTO file_sink SELECT * FROM q1_view")

    # Sink for filtered batches to Kafka topic "filtered-batches"
    # Serialize dict as JSON string
    filtered_batches_str = filtered_batches.map(lambda d: json.dumps(d), output_type=Types.STRING())
    kafka_sink_filtered = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_value_serialization_schema(SimpleStringSchema())
            .set_topic("filtered-batches")
            .build()
        ) \
        .build()
    filtered_batches_str.sink_to(kafka_sink_filtered)

    env.execute("Q1 Saturated Pixel Counter with Filtered Output")

    # The benchmark end and stopping polling thread are unreachable because env.execute blocks

if __name__ == "__main__":
    main()

