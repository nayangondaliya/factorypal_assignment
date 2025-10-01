"""
Kafka to Cassandra Streaming Pipeline

This module implements a streaming pipeline using Apache Spark to consume sensor data 
from a Kafka topic, compute per-minute average values, and store the aggregated results 
into a Cassandra database.

Modules:
- pyspark.sql: Spark session, DataFrame operations, schema definitions, and functions.
- cassandra.cluster: Cassandra connection and session handling.
- logging: Logging of errors and informational messages.

Pipeline Overview:
1. Establish a Spark session with Kafka and Cassandra support.
2. Read streaming sensor data from Kafka.
3. Parse JSON messages and compute 1-minute windowed average values.
4. Connect to Cassandra and ensure keyspace and table exist.
5. Write the aggregated data to Cassandra in a streaming manner.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, window, avg
from cassandra.cluster import Cluster, Session
import logging
from typing import Optional


def _create_spark_connection() -> Optional[SparkSession]:
    """
    Create a SparkSession configured for Kafka and Cassandra.

    Returns:
        Optional[SparkSession]: Configured Spark session instance or None if creation fails.
    """
    try:
        spark = (SparkSession.builder
                 .appName("KafkaToCassandraSensorPipeline")
                 .config("spark.jars.packages", 
                         "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7")
                 .config("spark.cassandra.connection.host", "cassandra")
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
              .option("startingOffsets", "latest") # Only read new messages
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
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json(col("value"), schema).alias("data"))
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
                col("sensorId").alias("sensor_id"),
                (col("window.start").cast("long") * 1000).alias("window_start"),
                (col("window.end").cast("long") * 1000).alias("window_end"),
                col("averageValue").alias("average_value")
            )
            )


def _create_cassandra_connection() -> Optional[Session]:
    """
    Establish a connection to Cassandra.

    Returns:
        Optional[Session]: Cassandra session object or None if connection fails.
    """
    try:
        cluster = Cluster(["cassandra"])
        return cluster.connect()
    except Exception as e:
        logging.error(f"Could not connect to Cassandra: {e}")
        return None


def _create_keyspace(session: Session) -> None:
    """
    Create the "sensor_stream" keyspace in Cassandra if it does not exist.

    Args:
        session (Session): Active Cassandra session.
    """
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS sensor_stream
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    """)
    logging.info("Keyspace created successfully!")


def _create_table(session: Session) -> None:
    """
    Create the "sensor_data" table in Cassandra if it does not exist.

    Args:
        session (Session): Active Cassandra session.
    """
    session.execute("""
        CREATE TABLE IF NOT EXISTS sensor_stream.sensor_data (
            sensor_id TEXT,
            window_start BIGINT,
            window_end BIGINT,
            average_value DOUBLE,
            PRIMARY KEY ((sensor_id), window_start, window_end)
        )
    """)
    logging.info("Table created successfully!")


def main() -> None:
    """
    Main function to orchestrate Spark streaming from Kafka to Cassandra.
    1. Create Spark session
    2. Read raw sensor data from Kafka
    3. Parse and aggregate data per sensor per minute
    4. Connect with cassandra database
    5. Create keyspace and table
    6. Write the streaming data to cassandra db
    """
        
    # Kafka configuration
    kafka_bootstrap_servers = "broker:29092"
    source_topic = "sensor-input"
    
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
    
    # Step 4: Connect with cassandra database
    session = _create_cassandra_connection()
    if not session:
        return

    # Step 5: Create keyspace and table
    _create_keyspace(session)
    _create_table(session)

    # Step 6: Write the streaming data to cassandra db
    query = (df_parsed.writeStream
                       .format("org.apache.spark.sql.cassandra")
                       .option("checkpointLocation", "/tmp/db_checkpoint")
                       .option("keyspace", "sensor_stream")
                       .option("table", "sensor_data")
                       .start())

    # Keep streaming until manually terminated
    query.awaitTermination()


if __name__ == "__main__":
    main()
