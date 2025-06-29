import json
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


def count_saturated_pixels(tiff_bytes, low_thresh=5000, high_thresh=65000):
    img = Image.open(io.BytesIO(tiff_bytes))
    arr = np.array(img)
    valid_pixels = arr[arr >= low_thresh]
    return np.sum(valid_pixels > high_thresh)


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
        return output.getvalue()


def main():
    config = Configuration()
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Kafka source from raw batches topic
    bootstrap_servers = ["kafka1:9092", "kafka2:9092"]
    source = KafkaSource.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_topics("tiff-batches") \
        .set_group_id("q1-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

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
            "batch_id": str(data["batch_id"]),
            "print_id": str(data["print_id"]),
            "tile_id": str(data["tile_id"]),
            "layer": str(data["layer"]),
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
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("q1-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_value_serialization_schema(SimpleStringSchema())
            .set_topic("q1-output")
            .build()
        ).build()
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
            'path' = 'file:///data/output/q1-csv',
            'format' = 'csv'
        )
    """)

    t_env.execute_sql("INSERT INTO file_sink SELECT * FROM q1_view")

    # Sink for filtered batches to Kafka topic "filtered-batches"
    # Serialize dict as JSON string
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

