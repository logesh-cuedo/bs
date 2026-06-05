#!/usr/bin/env python3
"""
IoT Streaming Pipeline — COW variant — EMR on EKS (Bronze + Silver)
Writes to *_cow tables in bs-iot-tables-poc S3 Tables bucket.

Changes vs streaming_v3_s3tables_eks-6exec.py (COW optimizations):
  - Silver tables now use COPY-ON-WRITE for merge/update/delete.
    Eliminates positional delete files, which were the root cause of S3 Tables
    auto-compaction conflicts (per AWS Support 2026-05-30). Each MERGE rewrites
    only the data files containing affected rows; old files are dereferenced.
    Reads from Athena/Spark become faster (no delete file merging at read time).
  - Bronze remains append-only, so the merge.mode property is irrelevant for
    bronze but kept for consistency.
  - Silver source uses repartition(SILVER_BUCKETS, "deviceID") instead of coalesce(1)
    so silver writes parallelise across the 8 bucket partitions.
  - maxOffsetsPerTrigger lowered 500_000 -> 150_000 to match ~4.2K rec/s input
    rate (per migration PDF). Smaller MERGE staging set per batch.
  - persist() uses MEMORY_AND_DISK (PySpark's MEMORY_AND_DISK is already in
    serialized form under the hood — the Scala MEMORY_AND_DISK_SER constant
    does not exist as a PySpark class attribute).
  - Iceberg auto-compaction table properties retained: manifest-merge + manifest
    target-size + write target-file-size + metadata cleanup. With COW, there are
    no positional delete files, so compaction primarily merges manifests and
    coalesces small data files.
  - Best-effort one-time Iceberg rewrite_data_files at startup. Wrapped in
    try/except — S3 Tables may parse-block the CALL (as it does for
    expire_snapshots); if so we log and rely on S3 Tables auto-compaction.
  - Commit retry slightly relaxed vs the merge-on-read version (15s -> 60s
    total) as a safety margin during initial COW rollout; with COW, actual
    conflict rate should drop sharply so this rarely fires.
  - Application-level retry on Iceberg ValidationException (data conflicts
    that Iceberg cannot auto-retry). 5 retries with exponential backoff +
    jitter (~2s -> ~32s, capped 60s). Per AWS blog "Manage concurrent write
    conflicts in Apache Iceberg" recommendation. This is a third layer of
    defense:
      Layer 1 (innermost): Iceberg auto-retries catalog commit conflicts
      Layer 2 (new middle): App retries ValidationException on each MERGE
      Layer 3 (outermost):  silver_failure_state tolerates N batch failures
  - No Spark config changes vs the existing job — all tuning is at the
    application or Iceberg-table-property level.
"""

from __future__ import annotations

import json
import logging
import random
import sys
import time
import traceback
from typing import Set

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from py4j.protocol import Py4JJavaError
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────
S3_BUCKET           = "emr-migration-poc"
KAFKA_BROKERS       = ("b-1.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-2.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-3.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092")
KAFKA_TOPIC         = "normalized-iot-events"
CONSUMER_GROUP      = "emr_eks_streaming_cow"

BASE                = f"s3://{S3_BUCKET}"
CHECKPOINT_COMBINED = f"{BASE}/checkpoints/eks/combined/iot_streaming_final_cow"
SCHEMA_PATH         = f"{BASE}/config/schemav1.avsc"
RENAME_MAP_PATH     = f"{BASE}/config/rename_mapv1.json"

CATALOG             = "s3tablescatalog/bs-iot-tables-poc"
BRONZE_TABLE        = f"`{CATALOG}`.bronze.iot_cow"
SILVER_LATEST_TABLE = f"`{CATALOG}`.silver.iot_events_latest_cow"
SILVER_VALID_TABLE  = f"`{CATALOG}`.silver.iot_events_latest_valid_cow"

REGION              = "ap-south-1"
CW_NAMESPACE        = "BatterySmart/IoTStreaming/COW"

BRONZE_COALESCE = 4
SILVER_BUCKETS  = 8

SILVER_MAX_CONSECUTIVE_FAILURES = 5

# Per-MERGE retry settings for ValidationException (data conflicts).
# Iceberg's built-in commit.retry.* handles catalog commit conflicts
# (CommitFailedException) automatically. ValidationException — which
# fires when concurrent writers touch overlapping data — cannot be
# auto-retried by Iceberg because it might cause data inconsistency.
# We retry it at the application layer with exponential backoff + jitter.
# Pattern per AWS blog "Manage concurrent write conflicts in Apache Iceberg".
MERGE_MAX_RETRIES = 5

# ─── Helpers ───────────────────────────────────────────────────────────────

def load_s3_text(path: str) -> str:
    assert path.startswith("s3://"), f"Expected s3:// path, got {path}"
    bucket, key = path[5:].split("/", 1)
    s3 = boto3.client("s3", region_name=REGION)
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def avsc_field_names(avsc_text: str) -> list[str]:
    schema = json.loads(avsc_text)
    return [f["name"] for f in schema.get("fields", [])]


def backoff(attempt: int) -> float:
    """Exponential backoff with jitter for ValidationException retries.

    attempt=1 -> ~2s,   attempt=2 -> ~4s,   attempt=3 -> ~8s,
    attempt=4 -> ~16s,  attempt=5 -> ~32s,  capped at 60s.
    Adds 0-25% random jitter so concurrent retries don't all fire at once.
    """
    exp = min(2 ** attempt, 60)
    jitter = random.uniform(0, 0.25 * exp)
    return exp + jitter


def is_validation_exception(java_exception) -> bool:
    """Walk the Java exception chain looking for Iceberg's ValidationException.

    This is the exception thrown when Iceberg's data-conflict check fails
    (Step 4 in the Iceberg write flow): a concurrent transaction modified
    files the current MERGE depends on. Unlike CommitFailedException,
    Iceberg's library cannot auto-retry this because retrying could cause
    data inconsistency — so we retry at the application layer.
    """
    cause = java_exception
    while cause is not None:
        try:
            if "org.apache.iceberg.exceptions.ValidationException" \
               in str(cause.getClass().getName()):
                return True
            cause = cause.getCause()
        except Exception:
            # Defensive: if exception chain traversal itself fails,
            # treat it as non-ValidationException.
            return False
    return False


def merge_with_retry(spark: SparkSession, sql: str, label: str) -> None:
    """Execute a MERGE INTO with retry on Iceberg ValidationException.

    Catalog-commit conflicts (CommitFailedException) are handled inside
    Iceberg via commit.retry.* table properties. This wrapper only kicks
    in for ValidationException — data conflicts that need application-side
    retries. After MERGE_MAX_RETRIES, the exception propagates up so the
    outer silver_failure_state counter can decide whether to abort the job.
    """
    attempt = 0
    while True:
        try:
            spark.sql(sql)
            return
        except Py4JJavaError as e:
            if not is_validation_exception(e.java_exception):
                # Not a ValidationException — let outer handler deal with it
                # (commit conflicts, etc., have their own table-property retries
                # already; if those exhausted, surfacing here is correct).
                raise
            attempt += 1
            if attempt >= MERGE_MAX_RETRIES:
                logger.error(
                    f"[{label}] ValidationException persisted after "
                    f"{MERGE_MAX_RETRIES} retries — propagating."
                )
                raise
            delay = backoff(attempt)
            logger.warning(
                f"[{label}] ValidationException on attempt {attempt}/{MERGE_MAX_RETRIES} "
                f"— retrying in {delay:.1f}s."
            )
            time.sleep(delay)


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


# ─── One-time pre-stream compaction ────────────────────────────────────────

def one_time_compact_silver(spark: SparkSession) -> None:
    """Best-effort compaction of silver tables before the stream starts.
    S3 Tables blocks some Iceberg CALL procedures at parse time (notably
    system.expire_snapshots); if rewrite_data_files is similarly blocked,
    log and continue — S3 Tables auto-compaction will pick up the slack."""
    for short_name in ("silver.iot_events_latest_cow", "silver.iot_events_latest_valid_cow"):
        try:
            if not table_exists(spark, f"`{CATALOG}`.{short_name}"):
                logger.info(f"[startup-compact] {short_name} does not exist yet — skipping.")
                continue
            logger.info(f"[startup-compact] Rewriting {short_name}...")
            spark.sql(f"""
                CALL `{CATALOG}`.system.rewrite_data_files(
                    table => '{short_name}',
                    strategy => 'binpack',
                    options => map('target-file-size-bytes', '134217728', 'min-input-files', '5')
                )
            """)
            logger.info(f"[startup-compact] Done: {short_name}")
        except Exception as e:
            logger.warning(
                f"[startup-compact] rewrite_data_files unavailable for {short_name} "
                f"— relying on S3 Tables auto-compaction. ({e})"
            )


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
        .appName("iot-streaming-cow")
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
        "maxOffsetsPerTrigger":    "150000",
    }

    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Checkpoint location: {CHECKPOINT_COMBINED}")
    logger.info(f"maxOffsetsPerTrigger: {kafka_params['maxOffsetsPerTrigger']}")
    logger.info(f"Target tables (COW): {BRONZE_TABLE}, {SILVER_LATEST_TABLE}, {SILVER_VALID_TABLE}")

    # Best-effort one-shot compaction before the stream starts.
    one_time_compact_silver(spark)

    spark.streams.addListener(ProgressListener(CW_NAMESPACE, REGION))

    raw = spark.readStream.format("kafka").options(**kafka_params).load()

    decoded = raw.select(
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*")

    # Iceberg table properties used on first-time create.
    #
    # COPY-ON-WRITE for merge/update/delete:
    #   - Eliminates positional delete files, which were AWS-confirmed root
    #     cause of S3 Tables auto-compaction conflicts on our silver tables.
    #   - Each MERGE rewrites only the data files containing affected rows;
    #     old files are dereferenced and cleaned up via snapshot expiration.
    #   - Athena/Spark reads get faster (no delete-file application at read).
    #   - Bronze is append-only so merge.mode is irrelevant for bronze, but
    #     kept for property consistency across all tables.
    #
    # Auto-compaction equivalents (table-level, applied on every commit):
    #   - delete-after-commit + previous-versions-max bound metadata file growth
    #   - manifest-merge + manifest target-size-bytes auto-merge small manifests
    #     on each commit
    #   - target-file-size-bytes nudges data files toward 128MB on write
    # Bulk data-file rewrite is still handled by S3 Tables auto-compaction
    # (see put-table-maintenance-configuration, set in Layer 1).
    common_table_props = {
        "format-version":                          "2",
        "write.merge.mode":                        "copy-on-write",
        "write.update.mode":                       "copy-on-write",
        "write.delete.mode":                       "copy-on-write",
        "write.merge.isolation-level":             "snapshot",
        "commit.retry.num-retries":                "5",
        "commit.retry.min-wait-ms":                "500",
        "commit.retry.max-wait-ms":                "10000",
        "commit.retry.total-timeout-ms":           "60000",
        # Auto-compaction equivalents (table-level, applied on every commit)
        "write.metadata.delete-after-commit.enabled": "true",
        "write.metadata.previous-versions-max":       "100",
        "commit.manifest.target-size-bytes":          "8388608",
        "commit.manifest-merge.enabled":              "true",
        "write.target-file-size-bytes":               "134217728",
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
            # repartition(SILVER_BUCKETS, "deviceID") — aligned with the bucket
            # partitioning on the target tables so the silver MERGE has 8-way
            # parallel writer tasks instead of single-task fanout.
            latest_rows_df = (agg.select("deviceID", "_latest.*")
                              .repartition(SILVER_BUCKETS, F.col("deviceID")))

            combined = (agg.select(
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
            ).repartition(SILVER_BUCKETS, F.col("deviceID")))

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.silver")

            # ── silver.iot_events_latest_valid_cow ───────────────────────
            if not table_exists(spark, SILVER_VALID_TABLE):
                apply_table_props(
                    combined.writeTo(SILVER_VALID_TABLE)
                            .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
            else:
                combined.createOrReplaceGlobalTempView("combined_valid_src_cow")
                merge_with_retry(spark, f"""
                    MERGE INTO {SILVER_VALID_TABLE} AS target
                    USING global_temp.combined_valid_src_cow AS source
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
                """, label=f"Silver-valid B{batch_id}")

            # ── silver.iot_events_latest_cow ─────────────────────────────
            if not table_exists(spark, SILVER_LATEST_TABLE):
                apply_table_props(
                    latest_rows_df.writeTo(SILVER_LATEST_TABLE)
                                  .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
            else:
                latest_rows_df.createOrReplaceGlobalTempView("latest_src_cow")
                merge_with_retry(spark, f"""
                    MERGE INTO {SILVER_LATEST_TABLE} AS target
                    USING global_temp.latest_src_cow AS source
                    ON target.deviceID = source.deviceID
                    WHEN MATCHED AND source.ts > target.ts THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """, label=f"Silver-latest B{batch_id}")

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
    logger.info("Starting combined Bronze+Silver COW stream...")
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