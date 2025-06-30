import json
import base64
import io
import numpy as np
from PIL import Image
import logging

from pyflink.common import Configuration, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext, ListStateDescriptor, MapFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, Schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Q2")

THRESHOLD = 6000
WINDOW_SIZE = 3  # number of layers in sliding window


def get_neighbors_coords():
    all_coords = []
    for dx in range(-4, 5):
        for dy in range(-4, 5):
            dist = abs(dx) + abs(dy)
            if 0 <= dist <= 4:
                all_coords.append((dx, dy))
    return all_coords


NEIGHBORS_0_2 = [p for p in get_neighbors_coords() if 0 <= abs(p[0]) + abs(p[1]) <= 2]
NEIGHBORS_3_4 = [p for p in get_neighbors_coords() if 2 < abs(p[0]) + abs(p[1]) <= 4]


class SlidingWindowProcessFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):
        descriptor = ListStateDescriptor(
            "layers_state",
            Types.PICKLED_BYTE_ARRAY()
        )
        self.layers_state = runtime_context.get_list_state(descriptor)

    def deserialize_layer(self, b64tif):
        tif_bytes = base64.b64decode(b64tif)
        img = Image.open(io.BytesIO(tif_bytes))
        arr = np.array(img)
        return arr

    def serialize_layer(self, arr):
        img = Image.fromarray(arr)
        with io.BytesIO() as output:
            img.save(output, format="TIFF")
            return base64.b64encode(output.getvalue()).decode("utf-8")

    def compute_deviation_per_point(self, layers):
        last_layer = layers[-1]
        h, w = last_layer.shape
        arr_3d = np.stack(layers, axis=0)
        deviations = np.zeros((h, w), dtype=np.float32)

        internal_offsets = NEIGHBORS_0_2
        external_offsets = NEIGHBORS_3_4

        for x in range(h):
            for y in range(w):
                internal_vals = []
                external_vals = []
                for dx, dy in internal_offsets:
                    for layer_idx in range(WINDOW_SIZE):
                        nx, ny = x + dx, y + dy
                        if 0 <= nx < h and 0 <= ny < w:
                            v = arr_3d[layer_idx, nx, ny]
                            if v != 0:
                                internal_vals.append(v)
                for dx, dy in external_offsets:
                    for layer_idx in range(WINDOW_SIZE):
                        nx, ny = x + dx, y + dy
                        if 0 <= nx < h and 0 <= ny < w:
                            v = arr_3d[layer_idx, nx, ny]
                            if v != 0:
                                external_vals.append(v)
                if internal_vals and external_vals:
                    mean_internal = np.mean(internal_vals)
                    mean_external = np.mean(external_vals)
                    deviations[x, y] = abs(mean_internal - mean_external)
                else:
                    deviations[x, y] = 0.0
        return deviations

    def process_element(self, value, ctx):
        try:
            data = json.loads(value)
            batch_id = int(data["batch_id"])
            print_id = data["print_id"]
            tile_id = int(data["tile_id"])
            layer = int(data["layer"])
            b64tif = data["tif"]

            arr = self.deserialize_layer(b64tif)

            current_layers = list(self.layers_state.get())
            if len(current_layers) >= WINDOW_SIZE:
                current_layers.pop(0)
            current_layers.append(arr)
            self.layers_state.update(current_layers)

            if len(current_layers) == WINDOW_SIZE:
                deviations = self.compute_deviation_per_point(current_layers)

                outliers = []
                for x in range(deviations.shape[0]):
                    for y in range(deviations.shape[1]):
                        if deviations[x, y] > THRESHOLD:
                            outliers.append(((x, y), float(deviations[x, y])))

                outliers.sort(key=lambda tup: tup[1], reverse=True)
                top5 = outliers[:5]

                row_values = [batch_id, print_id, tile_id]
                for p, d in top5:
                    row_values.append(f"{p[0]}_{p[1]}")
                    row_values.append(d)
                for _ in range(5 - len(top5)):
                    row_values.append("")
                    row_values.append(0.0)

                row = Row(*row_values)

                yield ("csv", row)

                outlier_points = [{"x": p[0], "y": p[1], "deviation": d} for p, d in outliers]
                q3_output = {
                    "batch_id": batch_id,
                    "print_id": print_id,
                    "tile_id": tile_id,
                    "outliers": outlier_points
                }

                yield ("q3", q3_output)

        except Exception as e:
            logger.error(f"Failed processing element for batch {data.get('batch_id', 'unknown')}, tile {data.get('tile_id', 'unknown')}: {e}")


def main():
    config = Configuration()
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    config.set_string("state.backend", "hashmap")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    bootstrap_servers = ["kafka1:9092", "kafka2:9092"]
    source = KafkaSource.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_topics("filtered-batches") \
        .set_group_id("q2-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

    keyed = ds.key_by(
        lambda x: (int(json.loads(x)["batch_id"]), int(json.loads(x)["tile_id"])),
        key_type=Types.TUPLE([Types.INT(), Types.INT()])
    )

    processed = keyed.process(
        SlidingWindowProcessFunction(),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.PICKLED_BYTE_ARRAY()
        ])
    )

    csv_stream = processed.filter(lambda x: x[0] == "csv") \
        .map(lambda x: x[1], output_type=Types.ROW_NAMED(
            ["batch_id", "print_id", "tile_id",
             "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"],
            [Types.INT(), Types.STRING(), Types.INT(),
             Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
             Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
             Types.STRING(), Types.FLOAT()]
        ))

    q3_stream = processed.filter(lambda x: x[0] == "q3") \
        .map(lambda x: json.dumps(x[1]), output_type=Types.STRING())

    kafka_sink_q2 = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("q2-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("q2-output")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    csv_stream.sink_to(kafka_sink_q2)

    kafka_sink_outlier = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("outlier-batches-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("outlier-batches")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ).build()

    q3_stream.sink_to(kafka_sink_outlier)

    # Create view for CSV data
    q2_table = t_env.from_data_stream(
        csv_stream,
        Schema.new_builder()
            .column("batch_id", DataTypes.INT())
            .column("print_id", DataTypes.STRING())
            .column("tile_id", DataTypes.INT())
            .column("P1", DataTypes.STRING())
            .column("dP1", DataTypes.FLOAT())
            .column("P2", DataTypes.STRING())
            .column("dP2", DataTypes.FLOAT())
            .column("P3", DataTypes.STRING())
            .column("dP3", DataTypes.FLOAT())
            .column("P4", DataTypes.STRING())
            .column("dP4", DataTypes.FLOAT())
            .column("P5", DataTypes.STRING())
            .column("dP5", DataTypes.FLOAT())
            .build()
    )

    t_env.create_temporary_view("q2_view", q2_table)

    t_env.execute_sql("""
        CREATE TABLE file_sink_q2 (
            batch_id INT,
            print_id STRING,
            tile_id INT,
            P1 STRING,
            dP1 FLOAT,
            P2 STRING,
            dP2 FLOAT,
            P3 STRING,
            dP3 FLOAT,
            P4 STRING,
            dP4 FLOAT,
            P5 STRING,
            dP5 FLOAT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///data/output/q2.csv',
            'format' = 'csv'
        )
    """)

    t_env.execute_sql("INSERT INTO file_sink_q2 SELECT * FROM q2_view")

    env.execute("Q2 Job")


if __name__ == "__main__":
    main()
