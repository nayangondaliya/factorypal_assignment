"""
Kafka to Kafka Streaming Pipeline

This module implements a streaming pipeline using Apache Spark to consume sensor data from a Kafka topic,
compute per-minute average values, and write the aggregated results to another Kafka topic.

Modules:
- pyspark.sql: Spark session, DataFrame operations, schema definitions, and functions.
- logging: Logging of errors and informational messages.

Pipeline Overview:
1. Establish a Spark session with Kafka support.
2. Read streaming sensor data from the source Kafka topic.
3. Parse JSON messages, convert timestamps, and compute 1-minute windowed averages.
4. Serialize the aggregated data as JSON and prepare Kafka key/value.
5. Write the aggregated data to the target Kafka topic with checkpointing for exactly-once semantics.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, to_json, struct, window, avg
import logging
from typing import Optional


def _create_spark_connection() -> Optional[SparkSession]:
    """
    Create a SparkSession configured for Kafka.

    Returns:
        Optional[SparkSession]: Configured Spark session instance or None if creation fails.
    """
    try:
        spark = (SparkSession.builder
                 .appName("KafkaToKafkaSensorPipeline")
                 .master("local[*]")
                 .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logging.error(f"Could not create Spark session: {e}")
        return None


def _read_from_kafka(spark: SparkSession, bootstrap_servers: str, topic: str) -> Optional[DataFrame]:
    """
    Read streaming data from a Kafka topic.

    Args:
        spark (SparkSession): Active Spark session.
        bootstrap_servers (str): Kafka bootstrap servers.
        topic (str): Source Kafka topic.

    Returns:
        Optional[DataFrame]: Streaming DataFrame with raw Kafka messages or None if reading fails.
    """
    try:
        df = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", bootstrap_servers)
              .option("subscribe", topic)
              .option("startingOffsets", "latest")  # Only read new messages
              .load())
        return df
    except Exception as e:
        logging.error(f"Failed to read from Kafka topic {topic}: {e}")
        return None


def _parse_and_aggregate_stream_dataset(spark_df: DataFrame) -> DataFrame:
    """
    Parse the raw Kafka messages (JSON) into structured format, convert timestamp,
    apply watermarking, and compute per-minute average values per sensor.
    
    Args:
        spark_df: Raw DataFrame from Kafka stream.

    Returns:
        Aggregated DataFrame with schema: sensor_id, windowStart, windowEnd, averageValue
    """
    schema = StructType([
        StructField("sensorId", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("timestamp", LongType(), True)
    ])

    df_parsed = (spark_df
                 .selectExpr("CAST(value AS STRING) as json_str")
                 .select(from_json(col("json_str"), schema).alias("data"))
                 .select("data.*")
                 .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))
                 )

    return (df_parsed
            .withWatermark("event_time", "1 minute")
            .groupBy(
                col("sensorId"),
                window(col("event_time"), "1 minute")
            )
            .agg(avg("value").alias("averageValue"))
            .select(
                col("sensorId"),
                (col("window.start").cast("long") * 1000).alias("windowStart"),
                (col("window.end").cast("long") * 1000).alias("windowEnd"),
                col("averageValue")
            )
            )

def _serialize_kafka_event(df: DataFrame) -> DataFrame:
    """
    Prepare the aggregated DataFrame to be written to Kafka.
    - Use sensor_id as key
    - Serialize the payload as JSON

    Args:
        df: Aggregated DataFrame with per-minute averages.

    Returns:
        DataFrame with columns "key" and "value" ready for Kafka sink.
    """
    return (df.withColumn("key", col("sensorId"))
              .withColumn("value", to_json(struct("sensorId", "averageValue", "windowStart", "windowEnd")))
              .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)"))


def main() -> None:
    """
    Orchestrates the Kafka-to-Kafka Spark streaming pipeline:
    1. Create Spark session
    2. Read raw sensor data from Kafka
    3. Parse and aggregate data per sensor per minute
    4. Serialize and write aggregated results back to Kafka
    """

    # Kafka configuration
    kafka_bootstrap_servers = "broker:29092"
    source_topic = "sensor-input"
    target_topic = "sensor-output"

    # Step 1: Create Spark session
    spark = _create_spark_connection()
    if not spark:
        return

    # Step 2: Read raw Kafka stream
    df_raw = _read_from_kafka(spark=spark, 
                                bootstrap_servers=kafka_bootstrap_servers,
                                topic=source_topic)
    if not df_raw:
        return

    # Step 3: Parse JSON and compute per-minute averages
    df_parsed = _parse_and_aggregate_stream_dataset(df_raw)

    # Step 4: Serialize the output for Kafka sink
    df_to_kafka = _serialize_kafka_event(df_parsed)

    # Step 5: Write the streaming data to Kafka
    query = (df_to_kafka.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
             .option("topic", target_topic)
             .option("checkpointLocation", "/tmp/kafka_checkpoint")
             .start())

    # Keep streaming until manually terminated
    query.awaitTermination()


if __name__ == "__main__":
    main()
