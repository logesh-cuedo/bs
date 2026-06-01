#!/usr/bin/env python3
"""
IoT Streaming Pipeline — TEST variant — EMR on EKS (Bronze + Silver)
Writes to *_test tables in bs-iot-tables-poc S3 Tables bucket.
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from typing import Set

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, struct, get_json_object, expr, bucket,
    from_utc_timestamp, to_date, year, month, day, round as F_round,
    max as F_max,
)
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────
S3_BUCKET           = "emr-migration-poc"
KAFKA_BROKERS       = ("b-1.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-2.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-3.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092")
KAFKA_TOPIC         = "normalized-iot-events"
CONSUMER_GROUP      = "emr_eks_streaming_poc_test"

BASE                = f"s3://{S3_BUCKET}"
CHECKPOINT_COMBINED = f"{BASE}/checkpoints/eks/combined/iot_v8_6exec"
SCHEMA_PATH         = f"{BASE}/config/schemav1.avsc"
RENAME_MAP_PATH     = f"{BASE}/config/rename_mapv1.json"

CATALOG             = "s3tablescatalog/bs-iot-tables-poc"
BRONZE_TABLE        = f"`{CATALOG}`.bronze.iot_test"
SILVER_LATEST_TABLE = f"`{CATALOG}`.silver.iot_events_latest_test"
SILVER_VALID_TABLE  = f"`{CATALOG}`.silver.iot_events_latest_valid_test"

REGION              = "ap-south-1"
CW_NAMESPACE        = "BatterySmart/IoTStreaming/Test"

BRONZE_COALESCE = 4
SILVER_COALESCE = 1
SILVER_BUCKETS  = 8

SILVER_MAX_CONSECUTIVE_FAILURES = 5

# ─── Helpers ───────────────────────────────────────────────────────────────

def load_s3_text(path: str) -> str:
    assert path.startswith("s3://"), f"Expected s3:// path, got {path}"
    bucket, key = path[5:].split("/", 1)
    s3 = boto3.client("s3", region_name=REGION)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def avsc_field_names(avsc_text: str) -> list[str]:
    schema = json.loads(avsc_text)
    return [f["name"] for f in schema.get("fields", [])]


_table_exists_cache: dict[str, bool] = {}

def table_exists(spark: SparkSession, table_name: str) -> bool:
    if _table_exists_cache.get(table_name):
        return True
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        _table_exists_cache[table_name] = True
        return True
    except Exception:
        return False


# ─── CloudWatch progress listener ──────────────────────────────────────────

class ProgressListener(StreamingQueryListener):
    """Publishes per-batch metrics to CloudWatch and warns on sustained lag.
    Disables itself silently on first CloudWatch failure (e.g. missing IAM)."""

    def __init__(self, namespace: str, region: str):
        self.namespace = namespace
        self.consecutive_behind = 0
        self.cw_disabled = False
        try:
            self.cw = boto3.client("cloudwatch", region_name=region)
        except Exception as e:
            logger.warning(f"CloudWatch client init failed; metrics disabled: {e}")
            self.cw = None
            self.cw_disabled = True

    def onQueryStarted(self, event):
        logger.info(f"Query started: id={event.id} name={event.name}")

    def onQueryProgress(self, event):
        p = event.progress
        input_rate = float(p.inputRowsPerSecond or 0.0)
        proc_rate  = float(p.processedRowsPerSecond or 0.0)
        num_input  = int(p.numInputRows or 0)
        durations  = p.durationMs or {}
        trigger_ms = int(durations.get("triggerExecution", 0))
        add_batch_ms = int(durations.get("addBatch", 0))

        if proc_rate > 0 and input_rate > proc_rate * 1.2:
            self.consecutive_behind += 1
            if self.consecutive_behind >= 3:
                logger.warning(
                    f"BACKLOG: {self.consecutive_behind} consecutive batches behind "
                    f"(input={input_rate:.0f}/s, processed={proc_rate:.0f}/s, "
                    f"trigger={trigger_ms}ms)"
                )
        else:
            self.consecutive_behind = 0

        if self.cw_disabled:
            return
        try:
            self.cw.put_metric_data(
                Namespace=self.namespace,
                MetricData=[
                    {"MetricName": "InputRowsPerSecond",     "Value": input_rate, "Unit": "Count/Second"},
                    {"MetricName": "ProcessedRowsPerSecond", "Value": proc_rate,  "Unit": "Count/Second"},
                    {"MetricName": "NumInputRows",           "Value": num_input,  "Unit": "Count"},
                    {"MetricName": "BatchDurationMs",        "Value": trigger_ms, "Unit": "Milliseconds"},
                    {"MetricName": "AddBatchDurationMs",     "Value": add_batch_ms, "Unit": "Milliseconds"},
                    {"MetricName": "ConsecutiveBehindBatches", "Value": self.consecutive_behind, "Unit": "Count"},
                ],
            )
        except (BotoCoreError, ClientError) as e:
            logger.warning(f"CloudWatch put_metric_data failed; disabling metrics: {e}")
            self.cw_disabled = True

    def onQueryTerminated(self, event):
        logger.info(f"Query terminated: id={event.id}")


# ─── Batch prep — shared timestamp conversion ──────────────────────────────

def prepare_batch(batch_df: DataFrame) -> DataFrame:
    """Compute latencies (epoch-ms math) then convert all four timestamps to IST.
    Shared by bronze and silver to avoid double conversion."""
    return (batch_df
        .withColumn("timeLatency",     F_round((col("pushedAt")   - col("ts"))       / (1000 * 60), 2))
        .withColumn("consumedLatency", F_round((col("consumedAt") - col("pushedAt")) / (1000 * 60), 2))
        .withColumn("ts",         from_utc_timestamp((col("ts")         / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("originalTs", from_utc_timestamp((col("originalTs") / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("consumedAt", from_utc_timestamp((col("consumedAt") / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("pushedAt",   from_utc_timestamp((col("pushedAt")   / 1000).cast("timestamp"), "Asia/Kolkata")))


# ─── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        _run()
    except Exception:
        print("STREAMING JOB FAILED (top-level):", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        raise


def _run() -> None:
    spark = (SparkSession.builder
        .appName("iot-streaming-bronze-silver-eks-test")
        .config("spark.sql.streaming.streamingProgressMaxRetained", "10")
        .config("spark.ui.retainedJobs", "50")
        .config("spark.ui.retainedStages", "50")
        .getOrCreate())

    logger.info("SparkSession ready. Loading schema and rename map from S3...")

    avro_schema = load_s3_text(SCHEMA_PATH)
    rename_map  = json.loads(load_s3_text(RENAME_MAP_PATH))
    expected_fields: Set[str] = set(rename_map.keys())

    col_order = avsc_field_names(avro_schema)
    col_order += ["timeLatency", "consumedLatency", "year", "month", "day", "insert_date"]

    kafka_params = {
        "kafka.bootstrap.servers": KAFKA_BROKERS,
        "subscribe":               KAFKA_TOPIC,
        "startingOffsets":         "latest",
        "failOnDataLoss":          "false",
        "groupIdPrefix":           CONSUMER_GROUP,
        "kafka.security.protocol": "PLAINTEXT",
        "maxOffsetsPerTrigger":    "500000",
    }

    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Checkpoint location: {CHECKPOINT_COMBINED}")
    logger.info(f"maxOffsetsPerTrigger: {kafka_params['maxOffsetsPerTrigger']}")
    logger.info(f"Target tables (TEST): {BRONZE_TABLE}, {SILVER_LATEST_TABLE}, {SILVER_VALID_TABLE}")

    spark.streams.addListener(ProgressListener(CW_NAMESPACE, REGION))

    raw = spark.readStream.format("kafka").options(**kafka_params).load()

    decoded = raw.select(
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*")

    # Iceberg table properties used on first-time create
    common_table_props = {
        "format-version":                "2",
        "write.merge.mode":              "merge-on-read",
        "write.update.mode":             "merge-on-read",
        "write.delete.mode":             "merge-on-read",
        "write.merge.isolation-level":   "snapshot",
        "commit.retry.num-retries":      "5",
        "commit.retry.min-wait-ms":      "500",
        "commit.retry.max-wait-ms":      "5000",
        "commit.retry.total-timeout-ms": "15000",
    }

    def apply_table_props(writer):
        w = writer
        for k, v in common_table_props.items():
            w = w.tableProperty(k, v)
        return w

    # ─── Bronze writer ─────────────────────────────────────────────────────
    def write_bronze(prepared_df: DataFrame, batch_id: int) -> None:
        bronze_df = (prepared_df
            .withColumn("insert_date", to_date("ts"))
            .withColumn("year",        year("ts"))
            .withColumn("month",       month("ts"))
            .withColumn("day",         day("ts"))
            .select(col_order))

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

        out_df = bronze_df.withColumn("alarms", struct(*struct_cols)).coalesce(BRONZE_COALESCE)

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.bronze")

        if table_exists(spark, BRONZE_TABLE):
            out_df.writeTo(BRONZE_TABLE).append()
        else:
            apply_table_props(
                out_df.writeTo(BRONZE_TABLE).partitionedBy("year", "month", "day")
            ).create()

        logger.info(f"[Bronze] Batch {batch_id}: written ({BRONZE_COALESCE} files)")

    # ─── Silver writer ─────────────────────────────────────────────────────
    def write_silver(prepared_df: DataFrame, batch_id: int) -> None:
        all_cols       = [c for c in prepared_df.columns if c not in ("deviceID", "ts")]
        bms_cols       = ["lat", "lon", "voltage", "soc", "current",
                          "temperature", "cellVolt", "cellTemp", "alarms"]
        iot_cols       = ["lat", "lon"]

        validity_df = (prepared_df
            .withColumn("isBMSValid",
                        F.when((col("voltage") > 0) & (col("voltage") < 70), 1).otherwise(0))
            .withColumn("isIoTValid",
                        F.when(col("lon") > 0, 1).otherwise(0)))

        # Single groupBy producing all three "latest per device" projections.
        agg = (validity_df
            .groupBy("deviceID")
            .agg(
                F_max(struct(col("ts"), *[col(c) for c in all_cols])).alias("_latest"),
                F_max(F.when(col("isBMSValid") == 1,
                             struct(col("ts"), *[col(c) for c in bms_cols]))).alias("_bms"),
                F_max(F.when(col("isIoTValid") == 1,
                             struct(col("ts"), *[col(c) for c in iot_cols]))).alias("_iot"),
            )).persist(StorageLevel.MEMORY_AND_DISK)

        try:
            latest_rows_df = agg.select("deviceID", "_latest.*").coalesce(SILVER_COALESCE)

            combined = agg.select(
                col("deviceID"),
                col("_latest.ts").alias("ts"),
                col("_bms.ts").alias("bmsLastTs"),
                col("_bms.lat").alias("bmsLastLat"),
                col("_bms.lon").alias("bmsLastLon"),
                col("_bms.voltage").alias("voltage"),
                col("_bms.soc").alias("soc"),
                col("_bms.current").alias("current"),
                col("_bms.temperature").alias("temperature"),
                col("_bms.cellVolt").alias("cellVolt"),
                col("_bms.cellTemp").alias("cellTemp"),
                col("_bms.alarms").alias("alarms"),
                col("_iot.ts").alias("iotLastTs"),
                col("_iot.lat").alias("lat"),
                col("_iot.lon").alias("lon"),
            ).coalesce(SILVER_COALESCE)

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.silver")

            # ── silver.iot_events_latest_valid_test ──────────────────────
            if not table_exists(spark, SILVER_VALID_TABLE):
                apply_table_props(
                    combined.writeTo(SILVER_VALID_TABLE)
                            .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
            else:
                combined.createOrReplaceGlobalTempView("combined_valid_src_test")
                spark.sql(f"""
                    MERGE INTO {SILVER_VALID_TABLE} AS target
                    USING global_temp.combined_valid_src_test AS source
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

            # ── silver.iot_events_latest_test ────────────────────────────
            if not table_exists(spark, SILVER_LATEST_TABLE):
                apply_table_props(
                    latest_rows_df.writeTo(SILVER_LATEST_TABLE)
                                  .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
            else:
                latest_rows_df.createOrReplaceGlobalTempView("latest_src_test")
                spark.sql(f"""
                    MERGE INTO {SILVER_LATEST_TABLE} AS target
                    USING global_temp.latest_src_test AS source
                    ON target.deviceID = source.deviceID
                    WHEN MATCHED AND source.ts > target.ts THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)

            logger.info(f"[Silver] Batch {batch_id}: upserted")
        finally:
            agg.unpersist()

    # ─── Combined processor ────────────────────────────────────────────────
    silver_failure_state = {"consecutive": 0}

    def process_combined(batch_df: DataFrame, batch_id: int) -> None:
        # Every 60 batches (~30 min at 30 s trigger): clear catalog metadata cache
        # to prevent slow accumulation in a long-running streaming app. Matches
        # the Databricks original's clear_catalog_cache pattern.
        if batch_id > 0 and batch_id % 60 == 0:
            try:
                spark.catalog.clearCache()
                logger.info(f"Batch {batch_id}: catalog cache cleared")
            except Exception as e:
                logger.warning(f"clearCache failed (continuing): {e}")

        prepared = prepare_batch(batch_df).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            if not prepared.take(1):
                logger.info(f"Batch {batch_id}: empty, skipping.")
                return

            write_bronze(prepared, batch_id)

            try:
                write_silver(prepared, batch_id)
                silver_failure_state["consecutive"] = 0
            except Exception as e:
                silver_failure_state["consecutive"] += 1
                fails = silver_failure_state["consecutive"]
                if fails >= SILVER_MAX_CONSECUTIVE_FAILURES:
                    logger.error(
                        f"[Silver] Batch {batch_id}: failed {fails}x consecutively — aborting: {e}"
                    )
                    raise
                logger.warning(
                    f"[Silver] Batch {batch_id}: write failed ({fails}/{SILVER_MAX_CONSECUTIVE_FAILURES}) "
                    f"— bronze written, next MERGE reconciles: {e}"
                )
        finally:
            prepared.unpersist()

    # ─── Start the stream ──────────────────────────────────────────────────
    logger.info("Starting combined Bronze+Silver TEST stream...")
    query = (decoded.writeStream
        .foreachBatch(process_combined)
        .outputMode("update")
        .option("checkpointLocation", CHECKPOINT_COMBINED)
        .trigger(processingTime="30 seconds")
        .start())

    logger.info("Stream running. Awaiting termination...")

    try:
        query.awaitTermination()
    except Exception as e:
        logger.error(f"awaitTermination raised: {e}")
        q_ex = query.exception()
        if q_ex:
            logger.error(f"Streaming query exception: {q_ex}")
            print(f"STREAMING QUERY EXCEPTION:\n{q_ex}", file=sys.stderr, flush=True)
        raise
    finally:
        try:
            if query.isActive:
                logger.info("Stopping streaming query...")
                query.stop()
        except Exception as e:
            logger.warning(f"Error stopping query: {e}")
        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    main()
