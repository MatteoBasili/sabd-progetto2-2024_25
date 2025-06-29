import json
import socket
import time
import base64
import io
import numpy as np
from PIL import Image

from pyflink.common import Configuration, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, Schema


def wait_for_kafka(host='kafka', port=9092):
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print("✅ Kafka is up.")
                break
        except OSError:
            print("⏳ Waiting for Kafka...")
            time.sleep(2)


def decode_tiff(tiff_b64):
    decoded = base64.b64decode(tiff_b64)
    img = Image.open(io.BytesIO(decoded))
    return np.array(img, dtype=np.int32)


def compute_deviation(tile_stack):
    outliers = []
    layers, height, width = tile_stack.shape

    for y in range(height):
        for x in range(width):
            center_vals = []
            outer_vals = []

            for l in range(3):
                for dy in range(-4, 5):
                    for dx in range(-4, 5):
                        ny, nx = y + dy, x + dx
                        d = abs(dx) + abs(dy)
                        if 0 <= ny < height and 0 <= nx < width and d != 0:
                            val = tile_stack[l, ny, nx]
                            if 0 <= d <= 2:
                                center_vals.append(val)
                            elif 2 < d <= 4:
                                outer_vals.append(val)

            if center_vals and outer_vals:
                avg_center = np.mean(center_vals)
                avg_outer = np.mean(outer_vals)
                deviation = abs(avg_center - avg_outer)
                if deviation > 6000:
                    outliers.append(((y, x), deviation))

    # Take top-5 points with highest deviation
    top_outliers = sorted(outliers, key=lambda x: -x[1])[:5]
    return top_outliers


def main():
    wait_for_kafka()

    config = Configuration()
    config.set_string("pipeline.jars",
                      "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("filtered-batches") \
        .set_group_id("q2-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="KafkaSource")

    tile_state = {}

    def process_batch(raw):
        data = json.loads(raw)
        batch_id = int(data["batch_id"])
        print_id = str(data["print_id"])
        tile_id = int(data["tile_id"])
        tile = decode_tiff(data["tif"])

        key = (print_id, tile_id)
        if key not in tile_state:
            tile_state[key] = []

        tile_state[key].append(tile)
        if len(tile_state[key]) > 3:
            tile_state[key].pop(0)

        if len(tile_state[key]) == 3:
            stack = np.stack(tile_state[key], axis=0)
            outliers = compute_deviation(stack)

            # For CSV and Kafka logging (Q2)
            row = [batch_id, print_id, tile_id]
            for (y, x), dev in outliers:
                row.append(f"({y},{x})")
                row.append(int(dev))
            while len(row) < 13:
                row.append("")
            out = Row(*row)

            # Also return raw outlier data for Q3
            outliers_json = []
            for (y, x), dev in outliers:
                outliers_json.append(json.dumps({
                    "batch_id": batch_id,
                    "print_id": print_id,
                    "tile_id": tile_id,
                    "x": x,
                    "y": y,
                    "deviation": dev
                }))

            return (out, outliers_json)

        else:
            return None

    # Process
    processed = ds.map(process_batch, output_type=Types.TUPLE([
        Types.ROW_NAMED(
            ["batch_id", "print_id", "tile_id", "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"],
            [Types.INT(), Types.STRING(), Types.INT()] + [Types.STRING(), Types.INT()] * 5
        ),
        Types.LIST(Types.STRING())
    ])).filter(lambda t: t is not None)

    # Split into streams
    parsed_rows = processed.map(lambda t: t[0], output_type=Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id", "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"],
        [Types.INT(), Types.STRING(), Types.INT()] + [Types.STRING(), Types.INT()] * 5
    ))

    json_outliers = processed.flat_map(
        lambda t: t[1], output_type=Types.STRING())

    # Write CSV strings to q2-output topic
    csv_strings = parsed_rows.map(lambda t: ",".join(map(str, t)), output_type=Types.STRING())

    kafka_sink_q2 = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("q2-output")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()
    csv_strings.sink_to(kafka_sink_q2)

    # Write raw outliers to outlier-batches topic for Q3
    kafka_sink_q3 = KafkaSink.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("outlier-batches")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()
    json_outliers.sink_to(kafka_sink_q3)

    # Also register table for file output
    t_env.create_temporary_view("q2_view", parsed_rows, Schema.new_builder()
        .column("batch_id", DataTypes.INT())
        .column("print_id", DataTypes.STRING())
        .column("tile_id", DataTypes.INT())
        .column("P1", DataTypes.STRING())
        .column("dP1", DataTypes.INT())
        .column("P2", DataTypes.STRING())
        .column("dP2", DataTypes.INT())
        .column("P3", DataTypes.STRING())
        .column("dP3", DataTypes.INT())
        .column("P4", DataTypes.STRING())
        .column("dP4", DataTypes.INT())
        .column("P5", DataTypes.STRING())
        .column("dP5", DataTypes.INT())
        .build())

    t_env.execute_sql("""
        CREATE TABLE q2_sink (
            batch_id INT,
            print_id STRING,
            tile_id INT,
            P1 STRING,
            dP1 INT,
            P2 STRING,
            dP2 INT,
            P3 STRING,
            dP3 INT,
            P4 STRING,
            dP4 INT,
            P5 STRING,
            dP5 INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///data/output/q2.csv',
            'format' = 'csv'
        )
    """)

    t_env.execute_sql("INSERT INTO q2_sink SELECT * FROM q2_view")

    env.execute("Q2 Outlier Detection + Export for Q3")


if __name__ == "__main__":
    main()

