import signal
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, to_date
from pyspark.sql.types import TimestampType

from config.logging_setup import setup_logger
from config.schema import (
    DEAD_LETTER_PARTITION_COLUMNS,
    DEAD_LETTER_REASON,
    DEVICE_TYPES,
    PARTITION_COLUMNS,
    STATUS_VALUES,
    build_spark_schema,
)

from config.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_REGION,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_STARTING_OFFSETS,
    LOG_DIR,
    S3_CHECKPOINT_PATH,
    S3_DEAD_LETTER_CHECKPOINT_PATH,
    S3_DEAD_LETTER_PATH,
    S3_OUTPUT_PATH,
    SPARK_TRIGGER_INTERVAL,
    validate_config,
)

from streaming.progress import PROGRESS_LOG_INTERVAL_SECONDS, format_query_progress

logger = setup_logger("spark_streaming", "spark_streaming.log")
_shutting_down = False


def _log_progress_loop(queries: list) -> None:
    while True:
        time.sleep(PROGRESS_LOG_INTERVAL_SECONDS)
        if _shutting_down:
            return
        for query in queries:
            if query.isActive:
                logger.info(format_query_progress(query))


def _shutdown(spark: SparkSession, queries: list, signum=None, frame=None) -> None:
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True

    logger.info("Shutdown requested — stopping streaming queries...")
    for query in queries:
        if query.isActive:
            query.stop()

    spark.stop()
    logger.info("Stopped.")
    sys.exit(0)


validate_config()

spark = (
    SparkSession.builder
    .appName("KafkaToS3Stream")
    .config(
        "spark.jars.packages",
        ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ])
    )
    .getOrCreate()
)

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
if AWS_SESSION_TOKEN:
    hadoop_conf.set("fs.s3a.session.token", AWS_SESSION_TOKEN)
hadoop_conf.set("fs.s3a.endpoint.region", AWS_REGION)

schema = build_spark_schema()

iot_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .load()
)

iot_raw = (
    iot_stream
    .selectExpr("CAST(value AS STRING) as raw_json")
    .select("raw_json", from_json(col("raw_json"), schema).alias("data"))
    .select("raw_json", "data.*")
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
)

is_valid = (
    col("device_id").isNotNull() & (col("device_id") != "") &
    col("device_type").isNotNull() & col("device_type").isin(*DEVICE_TYPES) &
    col("event_time").isNotNull() &
    col("value").isNotNull() &
    col("unit").isNotNull() &
    col("status").isNotNull() & col("status").isin(*STATUS_VALUES)
)

good_records = (
    iot_raw
    .filter(is_valid)
    .withColumn("event_date", to_date(col("event_time")))
    .drop("timestamp", "raw_json")
)

bad_records = (
    iot_raw
    .filter(~is_valid)
    .select(
        col("raw_json"),
        current_timestamp().alias("rejected_at"),
        lit(DEAD_LETTER_REASON).alias("reason"),
    )
    .withColumn("rejected_date", to_date(col("rejected_at")))
)

good_query = (
    good_records
    .writeStream
    .format("parquet")
    .option("path", S3_OUTPUT_PATH)
    .option("checkpointLocation", S3_CHECKPOINT_PATH)
    .outputMode("append")
    .partitionBy(*PARTITION_COLUMNS)
    .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
    .queryName("iot_good_records")
    .start()
)

dead_letter_query = (
    bad_records
    .writeStream
    .format("parquet")
    .option("path", S3_DEAD_LETTER_PATH)
    .option("checkpointLocation", S3_DEAD_LETTER_CHECKPOINT_PATH)
    .outputMode("append")
    .partitionBy(*DEAD_LETTER_PARTITION_COLUMNS)
    .trigger(processingTime=SPARK_TRIGGER_INTERVAL)
    .queryName("iot_dead_letter")
    .start()
)

active_queries = [good_query, dead_letter_query]

shutdown_handler = lambda signum, frame: _shutdown(spark, active_queries, signum, frame)
signal.signal(signal.SIGINT, shutdown_handler)
signal.signal(signal.SIGTERM, shutdown_handler)

progress_thread = threading.Thread(
    target=_log_progress_loop,
    args=(active_queries,),
    daemon=True,
    name="stream-progress-logger",
)
progress_thread.start()

logger.info("Streaming from Kafka (%s)", KAFKA_TOPIC)
logger.info("  good records → %s (partitions: %s)", S3_OUTPUT_PATH, ", ".join(PARTITION_COLUMNS))
logger.info("  dead letter  → %s (partitions: %s)", S3_DEAD_LETTER_PATH, ", ".join(DEAD_LETTER_PARTITION_COLUMNS))
logger.info("  log file     → %s", LOG_DIR / "spark_streaming.log")
logger.info("Progress logged every %ss. Ctrl+C to stop.", PROGRESS_LOG_INTERVAL_SECONDS)

try:
    spark.streams.awaitAnyTermination()
finally:
    if not _shutting_down:
        _shutdown(spark, active_queries)
