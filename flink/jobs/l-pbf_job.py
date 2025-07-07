import json
import logging
import csv
from io import StringIO

from pyflink.common import Configuration, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema

from queries import q1, q2, q3

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("L-PBF")


def to_csv(row):
    out = StringIO()
    ### DEBUG #############
    #logger.info(f"to_csv called with row: {row}")
    #######################
    csv.writer(out).writerow(row)
    return out.getvalue().strip()

def main():
    # Flink configuration
    config = Configuration()
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/kafka/flink-sql-connector-kafka-4.0.0-2.0.jar")
    config.set_string("state.backend", "rocksdb")
    config.set_string("state.checkpoints.dir", "file:///tmp/flink-checkpoints")
    
    ### For metrics evaluation ###
    config.set_string("metrics.latency.interval", "1000")  # ogni 1 secondo
    ##############################

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    #env.set_parallelism(2) 
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
 
    filtered_batches_json = processed.map(
        lambda x: json.dumps(x[1]),          
        output_type=Types.STRING()           
    )
    
    # Kafka sink for Q1 results    
    q1_csv_strings = q1_output.map(to_csv, output_type=Types.STRING())
    
    kafka_sink_q1 = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("q1-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("q1-output")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()
        
    q1_csv_strings.sink_to(kafka_sink_q1)

    # Q2 processing
    decoded_stream = filtered_batches_json.map(
        q2.decode_and_prepare,
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).filter(lambda x: x is not None)
    
    keyed_stream = decoded_stream.key_by(lambda x: (x["print_id"], x["tile_id"]))
    
    processed_q2 = keyed_stream.process(
        q2.SlidingWindowOutlierProcessFunction(),
        output_type=Types.TUPLE([
            Types.ROW_NAMED(
                ["batch_id","print_id","tile_id",
                 "p1","dp1","p2","dp2","p3","dp3","p4","dp4","p5","dp5"],
                [Types.INT(), Types.STRING(), Types.INT()] + [Types.STRING(), Types.STRING()] * 5
            ),
            Types.STRING()
        ])
    )
    
    q2_csv_output = processed_q2.map(lambda x: x[0], output_type=Types.ROW_NAMED(
        ["batch_id","print_id","tile_id","p1","dp1","p2","dp2","p3","dp3","p4","dp4","p5","dp5"],
        [Types.INT(), Types.STRING(), Types.INT()] + [Types.STRING(), Types.STRING()] * 5
    ))

    # Kafka sink for Q2 results
    q2_csv_strings = q2_csv_output.map(to_csv, output_type=Types.STRING())
    
    kafka_sink_q2 = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("q2-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("q2-output")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()
    
    q2_csv_strings.sink_to(kafka_sink_q2)

    # Q2 processing
    q3_rows = processed_q2 \
        .map(lambda x: x[1],               
             output_type=Types.STRING()) \
        .map(q3.process_json,              
             output_type=Types.ROW_NAMED(
                 ["batch_id", "print_id", "tile_id", "saturated", "centroids"],
                 [Types.INT(), Types.STRING(), Types.INT(), Types.INT(), Types.STRING()]
             )) \
        .filter(lambda r: r is not None)

    # Kafka sink for Q3 results        
    q3_csv_strings = q3_rows.map(to_csv, output_type=Types.STRING())

    kafka_sink_q3 = KafkaSink.builder() \
        .set_bootstrap_servers(",".join(bootstrap_servers)) \
        .set_transactional_id_prefix("l-pbf-output-") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("l-pbf-output")      
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ).build()

    q3_csv_strings.sink_to(kafka_sink_q3)

    env.execute("L-PBF Job")

if __name__ == "__main__":
    main()

