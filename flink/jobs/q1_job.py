import json
import base64
import io
import numpy as np
from PIL import Image, UnidentifiedImageError
import logging

from pyflink.common import Configuration, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, Schema

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q1")

# Threshold parameters
LOW_THRESH = 5000
HIGH_THRESH = 65000


def count_saturated_pixels(tiff_bytes, low_thresh=LOW_THRESH, high_thresh=HIGH_THRESH):
    """Count the number of pixels above the saturation threshold."""
    try:
        img = Image.open(io.BytesIO(tiff_bytes))
        arr = np.array(img)
        valid_pixels = arr[arr >= low_thresh]
        return int(np.sum(valid_pixels > high_thresh))
    except Exception as e:
        logger.error(f"Error while counting saturated pixels: {e}")
        return 0


def filter_tile_pixels(tiff_bytes, low_thresh=LOW_THRESH, high_thresh=HIGH_THRESH, batch_id=None, tile_id=None):
    """Filter pixels outside the defined valid range by setting them to 0."""
    try:
        img = Image.open(io.BytesIO(tiff_bytes))
        arr = np.array(img)

        mask = (arr >= low_thresh) & (arr <= high_thresh)
        # Avoid unnecessary cast if already uint16
        if arr.dtype != np.uint16:
            filtered_arr = np.where(mask, arr, 0).astype(np.uint16)
        else:
            filtered_arr = np.where(mask, arr, 0)

        filtered_img = Image.fromarray(filtered_arr)
        with io.BytesIO() as output:
            filtered_img.save(output, format="TIFF")
            return output.getvalue()
    except Exception as e:
        logger.error(f"[filter_tile_pixels] Error for batch_id={batch_id}, tile_id={tile_id}: {e}")
        return b""


def process_batch(raw):
    """Parse and process a single input batch, returning Q1 metrics and a filtered version."""
    try:
        data = json.loads(raw)

        required_fields = ["batch_id", "print_id", "tile_id", "layer", "tif"]
        if not all(k in data for k in required_fields):
            logger.warning(f"Incomplete data received: {data}")
            return None

        batch_id = int(data["batch_id"])
        tile_id = int(data["tile_id"])

        tif_bytes = base64.b64decode(data["tif"])
        saturated = count_saturated_pixels(tif_bytes)
        filtered_bytes = filter_tile_pixels(tif_bytes, batch_id=batch_id, tile_id=tile_id)
        filtered_b64 = base64.b64encode(filtered_bytes).decode("utf-8")

        q1_row = Row(
            batch_id=batch_id,
            print_id=str(data["print_id"]),
            tile_id=tile_id,
            saturated=saturated
        )

        filtered_batch = {
            "batch_id": str(batch_id),
            "print_id": str(data["print_id"]),
            "tile_id": str(tile_id),
            "layer": str(data["layer"]),
            "tif": filtered_b64
        }

        return q1_row, filtered_batch

    except Exception as e:
        logger.error(f"[process_batch] Failed to process batch: {e}")
        return None


def main():
    # Flink configuration
    config = Configuration()
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    config.set_string("state.backend", "hashmap")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka source configuration
    bootstrap_servers = ["kafka1:9092", "kafka2:9092"]
    source = KafkaSource.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_topics("tiff-batches") \
        .set_group_id("q1-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

    # Apply processing logic only to valid batches
    processed = ds.map(process_batch, output_type=Types.TUPLE([
        Types.ROW_NAMED(
            ["batch_id", "print_id", "tile_id", "saturated"],
            [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
        ),
        Types.MAP(Types.STRING(), Types.STRING())
    ])).filter(lambda x: x is not None)

    # Extract Q1 metrics and filtered images
    q1_output = processed.map(lambda x: x[0], output_type=Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id", "saturated"],
        [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
    ))

    filtered_batches = processed.map(lambda x: x[1], output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Kafka sink for Q1 results
    q1_csv_strings = q1_output.map(lambda t: ",".join(map(str, t)), output_type=Types.STRING())
    kafka_sink_q1 = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("q1-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_value_serialization_schema(SimpleStringSchema())
                .set_topic("q1-output")
                .build()
        ).build()
    q1_csv_strings.sink_to(kafka_sink_q1)

    # File sink for Q1 results (CSV)
    t_env.create_temporary_view(
        "q1_view", q1_output,
        Schema.new_builder()
            .column("batch_id", DataTypes.INT())
            .column("print_id", DataTypes.STRING())
            .column("tile_id", DataTypes.INT())
            .column("saturated", DataTypes.INT())
            .build()
    )
    t_env.execute_sql("""
        CREATE TABLE file_sink_q1 (
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
    t_env.execute_sql("INSERT INTO file_sink_q1 SELECT * FROM q1_view")

    # Kafka sink for filtered image batches
    filtered_batches_str = filtered_batches.map(lambda d: json.dumps(d), output_type=Types.STRING())
    kafka_sink_filtered = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("filtered-batches-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_value_serialization_schema(SimpleStringSchema())
                .set_topic("filtered-batches")
                .build()
        ).build()
    filtered_batches_str.sink_to(kafka_sink_filtered)

    env.execute("Q1 Job")


if __name__ == "__main__":
    main()

