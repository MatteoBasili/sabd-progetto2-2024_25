import json
import base64
import io
import logging

from pyflink.common import Configuration, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, OutputTag
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema

from queries import q1, q2

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("L-PBF")

def main():
    # Flink configuration
    config = Configuration()
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    config.set_string("state.backend", "hashmap")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(5000)

    # Kafka source
    bootstrap_servers = ["kafka1:29092", "kafka2:29093"]
    source = KafkaSource.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_topics("tiff-batches") \
        .set_group_id("l-pbf-group") \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "KafkaSource")

    # Q1 processing
    processed = ds.map(q1.process_batch, output_type=Types.TUPLE([
        Types.ROW_NAMED(
            ["batch_id", "print_id", "tile_id", "saturated"],
            [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
        ),
        Types.MAP(Types.STRING(), Types.STRING())
    ])).filter(lambda x: x is not None)

    q1_output = processed.map(lambda x: x[0], output_type=Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id", "saturated"],
        [Types.INT(), Types.STRING(), Types.INT(), Types.INT()]
    ))

    filtered_batches = processed.map(lambda x: x[1], output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # Q2 processing
    filtered_batches_str = filtered_batches.map(lambda d: json.dumps(d), output_type=Types.STRING())

    keyed_batches = filtered_batches_str.key_by(
        lambda x: (int(json.loads(x)["batch_id"]), int(json.loads(x)["tile_id"])),
        key_type=Types.TUPLE([Types.INT(), Types.INT()])
    )

    q2_result = keyed_batches.process(q2.SlidingWindowProcessFunction())

    q2_output_type_info = Types.ROW_NAMED(
        ["batch_id", "print_id", "tile_id",
         "P1", "dP1", "P2", "dP2", "P3", "dP3", "P4", "dP4", "P5", "dP5"],
        [Types.INT(), Types.STRING(), Types.INT(),
         Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
         Types.STRING(), Types.FLOAT(), Types.STRING(), Types.FLOAT(),
         Types.STRING(), Types.FLOAT()]
    )

    q2_output_stream = q2_result.get_side_output(q2.q2_output_tag).map(lambda x: x, output_type=q2_output_type_info)
    
    outliers_stream = q2_result.get_side_output(q2.outliers_output_tag)

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

    # Kafka sink for Q2 results
    #kafka_sink_q2 = KafkaSink.builder() \
     #   .set_bootstrap_servers(",".join(bootstrap_servers)) \
      #  .set_transactional_id_prefix("q2-output-") \
       # .set_record_serializer(
        #    KafkaRecordSerializationSchema.builder()
         #       .set_topic("q2-output")
          #      .set_value_serialization_schema(SimpleStringSchema())
           #     .build()
        #).build()
    #q2_output_stream.map(lambda row: ",".join(map(str, row)), output_type=Types.STRING()) \
     #   .sink_to(kafka_sink_q2)

    # Kafka sink for outlier JSON
    #kafka_sink_outliers = KafkaSink.builder() \
     #   .set_bootstrap_servers(",".join(bootstrap_servers)) \
      #  .set_transactional_id_prefix("outlier-batches-") \
       # .set_record_serializer(
        #    KafkaRecordSerializationSchema.builder()
         #       .set_topic("outlier-batches")
          #      .set_value_serialization_schema(SimpleStringSchema())
           #     .build()
        #).build()
    #outliers_stream.sink_to(kafka_sink_outliers)

    env.execute("L-PBF Job")

if __name__ == "__main__":
    main()

