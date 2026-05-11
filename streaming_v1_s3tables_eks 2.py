#!/usr/bin/env python3
"""
IoT Streaming Pipeline — EMR on EKS (Bronze + Silver) — CLIENT
Reads from MSK Kafka, writes to S3 Table Bucket bs-iot-tables-poc.

Single-stream consolidated version:
  - One readStream → one foreachBatch handling bronze + silver_valid + silver_latest
  - Decoded batch cached once and reused (Avro decode happens once, not twice)
  - df.count() calls removed (they were silently double-running the pipeline)
  - Bronze schema preserved exactly (only `ts` → IST; other timestamp fields stay
    as raw longs) for apples-to-apples validation against Databricks Delta Lake.
"""
import json
import logging

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, struct, get_json_object,
    from_utc_timestamp, to_date, year, month, day, round, row_number
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

S3_BUCKET               = "emr-migration-poc"
KAFKA_BROKERS           = "b-1.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,b-2.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,b-3.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092"
KAFKA_TOPIC             = "normalized-iot-events"
CONSUMER_GROUP          = "emr_eks_streaming_poc"

BASE                    = f"s3://{S3_BUCKET}"
CHECKPOINT_COMBINED     = f"{BASE}/checkpoints/eks/combined/iot_v2"
SCHEMA_PATH             = f"{BASE}/config/schemav1.avsc"
RENAME_MAP_PATH         = f"{BASE}/config/rename_mapv1.json"

BRONZE_TABLE            = "`s3tablescatalog/bs-iot-tables-poc`.bronze.iot"
SILVER_LATEST_TABLE     = "`s3tablescatalog/bs-iot-tables-poc`.silver.iot_events_latest"
SILVER_VALID_TABLE      = "`s3tablescatalog/bs-iot-tables-poc`.silver.iot_events_latest_valid"


def load_from_s3(spark, path):
    return spark.sparkContext.wholeTextFiles(path).collect()[0][1]


def avsc_field_names(avsc_text):
    """Parse .avsc as plain JSON and return list of top-level field names."""
    schema = json.loads(avsc_text)
    return [f["name"] for f in schema.get("fields", [])]


def table_exists(spark, table_name):
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def clear_cache(spark, batch_id):
    if batch_id % 60 == 0 and batch_id > 0:
        logger.info(f"Clearing catalog cache at batch {batch_id}")
        spark.catalog.clearCache()


def main():
    try:
        _run()
    except Exception:
        import sys, traceback
        print("STREAMING QUERY FAILED:", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        raise


def _run():
    spark = SparkSession.builder \
        .appName("iot-streaming-bronze-silver-eks") \
        .config("spark.sql.shuffle.partitions", "32") \
        .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false") \
        .config("spark.sql.streaming.stateStore.compression.codec", "zstd") \
        .config("spark.sql.streaming.minBatchesToRetain", "10") \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "2min") \
        .config("spark.sql.streaming.stateStore.timeout", "10min") \
        .config("spark.sql.streaming.streamingProgressMaxRetained", "10") \
        .config("spark.ui.retainedJobs", "50") \
        .config("spark.ui.retainedStages", "50") \
        .config("spark.cleaner.ttl", "3600") \
        .getOrCreate()

    logger.info("SparkSession ready. Loading schema and rename map from S3...")

    avro_schema     = load_from_s3(spark, SCHEMA_PATH)
    rename_map      = json.loads(load_from_s3(spark, RENAME_MAP_PATH))
    expected_fields = set(rename_map.keys())

    col_order = avsc_field_names(avro_schema)
    col_order += ["timeLatency", "consumedLatency", "year", "month", "day", "insert_date"]

    kafka_params = {
        "kafka.bootstrap.servers": KAFKA_BROKERS,
        "subscribe":               KAFKA_TOPIC,
        "startingOffsets":         "latest",
        "failOnDataLoss":          "false",
        "groupIdPrefix":           CONSUMER_GROUP,
        "kafka.security.protocol": "PLAINTEXT",
        "maxOffsetsPerTrigger":    "150000",
    }

    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")

    raw = spark.readStream.format("kafka").options(**kafka_params).load()

    decoded = raw.select(
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*")

    def write_bronze(batch_df, batch_id):
        bronze_df = batch_df \
            .withColumn("timeLatency",     round((col("pushedAt") - col("ts")) / (1000 * 60), 2)) \
            .withColumn("consumedLatency", round((col("consumedAt") - col("pushedAt")) / (1000 * 60), 2)) \
            .withColumn("ts",              (col("ts") / 1000).cast("timestamp")) \
            .withColumn("ts",              from_utc_timestamp("ts", "Asia/Kolkata")) \
            .withColumn("insert_date",     to_date("ts")) \
            .withColumn("year",            year("ts")) \
            .withColumn("month",           month("ts")) \
            .withColumn("day",             day("ts")) \
            .select(col_order)

        from pyspark.sql.types import StructType
        alarms_type = bronze_df.schema["alarms"].dataType

        if isinstance(alarms_type, StructType):
            alarm_fields = {f.name for f in alarms_type.fields}
            unexpected = alarm_fields - expected_fields
            if unexpected:
                raise ValueError(f"Unexpected alarm fields: {unexpected}")
            struct_cols = [
                col(f"alarms.{f.name}").cast("string").alias(rename_map[f.name])
                for f in alarms_type.fields
            ]
        else:
            struct_cols = [
                get_json_object(col("alarms"), f"$.{k}").alias(rename_map[k])
                for k in sorted(expected_fields)
            ]

        out_df = bronze_df.withColumn("alarms", struct(*struct_cols))
        spark.sql("CREATE NAMESPACE IF NOT EXISTS `s3tablescatalog/bs-iot-tables-poc`.bronze")

        if table_exists(spark, BRONZE_TABLE):
            out_df.writeTo(BRONZE_TABLE).append()
        else:
            out_df.writeTo(BRONZE_TABLE) \
                .partitionedBy("year", "month", "day") \
                .tableProperty("format-version", "2") \
                .create()

        logger.info(f"[Bronze] Batch {batch_id}: written")

    def write_silver(batch_df, batch_id):
        silver_df = batch_df \
            .withColumn("ts",         (col("ts") / 1000).cast("timestamp")) \
            .withColumn("originalTs", (col("originalTs") / 1000).cast("timestamp")) \
            .withColumn("consumedAt", (col("consumedAt") / 1000).cast("timestamp")) \
            .withColumn("pushedAt",   (col("pushedAt") / 1000).cast("timestamp")) \
            .withColumn("ts",         from_utc_timestamp("ts", "Asia/Kolkata")) \
            .withColumn("originalTs", from_utc_timestamp("originalTs", "Asia/Kolkata")) \
            .withColumn("consumedAt", from_utc_timestamp("consumedAt", "Asia/Kolkata")) \
            .withColumn("pushedAt",   from_utc_timestamp("pushedAt", "Asia/Kolkata"))

        window_spec = Window.partitionBy("deviceID").orderBy(col("ts").desc())

        latest_rows_df = silver_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")

        valid_df = silver_df \
            .withColumn("isBMSValid",
                        F.when((F.col("voltage") > 0) & (F.col("voltage") < 70), 1).otherwise(0)) \
            .withColumn("isIoTValid",
                        F.when(F.col("lon") > 0, 1).otherwise(0))

        bms_valid_df = valid_df.filter(col("isBMSValid") == 1) \
            .select("deviceID", "ts", "lat", "lon", "voltage", "soc", "current",
                    "temperature", "cellVolt", "cellTemp", "alarms") \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1).drop("row_num") \
            .withColumnsRenamed({"ts": "bmsLastTs", "lat": "bmsLastLat", "lon": "bmsLastLon"})

        iot_valid_df = valid_df.filter(col("isIoTValid") == 1) \
            .select("deviceID", "ts", "lat", "lon") \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1).drop("row_num") \
            .withColumnRenamed("ts", "iotLastTs")

        latest = latest_rows_df.select("deviceID", "ts")
        combined = latest \
            .join(bms_valid_df, "deviceID", "left") \
            .join(iot_valid_df, "deviceID", "left")

        spark.sql("CREATE NAMESPACE IF NOT EXISTS `s3tablescatalog/bs-iot-tables-poc`.silver")

        if not table_exists(spark, SILVER_VALID_TABLE):
            combined.writeTo(SILVER_VALID_TABLE) \
                .tableProperty("format-version", "2") \
                .create()
        else:
            combined.createOrReplaceGlobalTempView("combined_valid_src")
            spark.sql(f"""
                MERGE INTO {SILVER_VALID_TABLE} AS target
                USING global_temp.combined_valid_src AS source
                ON target.deviceID = source.deviceID
                WHEN MATCHED THEN UPDATE SET
                    ts          = CASE WHEN target.ts IS NULL OR source.ts > target.ts THEN source.ts ELSE target.ts END,
                    bmsLastTs   = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastTs ELSE target.bmsLastTs END,
                    voltage     = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.voltage ELSE target.voltage END,
                    temperature = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.temperature ELSE target.temperature END,
                    current     = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.current ELSE target.current END,
                    soc         = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.soc ELSE target.soc END,
                    cellVolt    = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.cellVolt ELSE target.cellVolt END,
                    cellTemp    = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.cellTemp ELSE target.cellTemp END,
                    alarms      = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.alarms ELSE target.alarms END,
                    bmsLastLat  = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastLat ELSE target.bmsLastLat END,
                    bmsLastLon  = CASE WHEN target.bmsLastTs IS NULL OR source.bmsLastTs > target.bmsLastTs THEN source.bmsLastLon ELSE target.bmsLastLon END,
                    iotLastTs   = CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.iotLastTs ELSE target.iotLastTs END,
                    lat         = CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.lat ELSE target.lat END,
                    lon         = CASE WHEN target.iotLastTs IS NULL OR source.iotLastTs > target.iotLastTs THEN source.lon ELSE target.lon END
                WHEN NOT MATCHED THEN INSERT *
            """)

        if not table_exists(spark, SILVER_LATEST_TABLE):
            latest_rows_df.writeTo(SILVER_LATEST_TABLE) \
                .tableProperty("format-version", "2") \
                .create()
        else:
            latest_rows_df.createOrReplaceGlobalTempView("latest_src")
            spark.sql(f"""
                MERGE INTO {SILVER_LATEST_TABLE} AS target
                USING global_temp.latest_src AS source
                ON target.deviceID = source.deviceID
                WHEN MATCHED AND source.ts > target.ts THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        logger.info(f"[Silver] Batch {batch_id}: upserted")

    def process_combined(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            logger.info(f"Batch {batch_id}: empty, skipping.")
            return

        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            write_bronze(batch_df, batch_id)
            write_silver(batch_df, batch_id)
            clear_cache(spark, batch_id)
        finally:
            batch_df.unpersist()

    logger.info("Starting combined Bronze+Silver stream...")
    query = decoded.writeStream \
        .foreachBatch(process_combined) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_COMBINED) \
        .trigger(processingTime="30 seconds") \
        .start()

    logger.info("Stream running. Awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
