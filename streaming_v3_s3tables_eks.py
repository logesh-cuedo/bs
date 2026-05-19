#!/usr/bin/env python3
"""
IoT Streaming Pipeline — EMR on EKS (Bronze + Silver) — v3 (FINAL)
==================================================================
Reads from MSK Kafka, writes to S3 Table Bucket bs-iot-tables-poc.

DEPLOYMENT MODEL:
  - Deployment-level configs live in job-submit-streaming-v3.json
    (executor resources, K8s settings, NodePool selectors, RocksDB tuning,
     decommissioning, network timeouts, serializer, etc.)
  - Script-level configs in SparkSession.builder.config() are kept MINIMAL —
    only things genuinely script-specific or not yet covered by JSON.
    All duplicates between script and JSON have been removed; JSON wins.

CHANGES FROM v1 (streaming_v1_s3tables_eks.py):

PERFORMANCE
  - Replaced 3× row_number().over(window) with groupBy + max(struct(...))
    — eliminates 2 of 3 shuffles + sorts per batch (~2× batch speed)
  - Removed batch_df.rdd.isEmpty() (full pass); use take(1) instead
  - table_exists() caches positive results (no more DESCRIBE per batch)
  - coalesce(N) before writes to reduce small-file count downstream
  - Schema/rename map loaded via boto3 (no Spark job for a 1KB file)

CORRECTNESS / SAFETY
  - Removed spark.cleaner.ttl=3600 (deprecated, risks silent data loss)
  - Removed periodic spark.catalog.clearCache() (masks real leaks)
  - Try/except around streaming start surfaces query exceptions
  - Bronze and Silver write errors logged separately

KAFKA / STREAMING
  - maxOffsetsPerTrigger 150K → 500K (allows catch-up; absorbs bursts)
  - New checkpoint path (iot_v3) so JSON config values actually take
    effect (old checkpoint forced shuffle=16 + HDFSBackedStateStore)

BRONZE SCHEMA PRESERVED
  - write_bronze converts only `ts` to TIMESTAMP; originalTs/consumedAt/
    pushedAt stay as BIGINT (matches existing Iceberg table schema).
  - write_silver does its own conversions of all 4 fields for MERGE logic.

INFRASTRUCTURE (handled in JSON, not here):
  - Pods land on dedicated emr NodePool with toleration for
    upgrid.in/nodepool=emr:NoSchedule
  - On-demand capacity (no Spot interruptions)
  - amd64 arch only (NodePool allows both; we lock to amd64 since past
    runs all ran on x86_64)
  - m or r instance categories (excluding burstable t-class)
  - Graceful decommissioning with S3 fallback storage
  - Network resilience: timeout 300s, retries 8, FAIR scheduler
  - RocksDB state store with changelog checkpointing and compactOnCommit
"""

from __future__ import annotations

import json
import logging
import sys
import traceback
from typing import Set

import boto3
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, struct, get_json_object,
    from_utc_timestamp, to_date, year, month, day, round as F_round,
    max as F_max,
)
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
CONSUMER_GROUP      = "emr_eks_streaming_poc"

BASE                = f"s3://{S3_BUCKET}"
# Fresh checkpoint: silver cadence + topology changed; can't reuse v5 state.
# Cost: stream replays from earliest available Kafka offsets on first start.
CHECKPOINT_COMBINED = f"{BASE}/checkpoints/eks/combined/iot_v6"
SCHEMA_PATH         = f"{BASE}/config/schemav1.avsc"
RENAME_MAP_PATH     = f"{BASE}/config/rename_mapv1.json"

CATALOG             = "s3tablescatalog/bs-iot-tables-poc"
BRONZE_TABLE        = f"`{CATALOG}`.bronze.iot"
SILVER_LATEST_TABLE = f"`{CATALOG}`.silver.iot_events_latest"
SILVER_VALID_TABLE  = f"`{CATALOG}`.silver.iot_events_latest_valid"

# Output partition control — keep file counts manageable.
# Bronze: ~3,500 rec/s × 30s = ~105K rec/batch. coalesce(4) → ~26K rec/file.
# Silver: after dedup, much smaller → coalesce(1) is fine.
BRONZE_COALESCE = 4
SILVER_COALESCE = 1

# Silver cadence — Iceberg MERGE INTO is expensive and creates one snapshot per
# commit. At 30s trigger × 24h that's ~2,880 snapshots/day per silver table —
# which is what bloated MERGE planning in run 000000037gscnp2qanr.
# Running silver once every 10 bronze batches = once every 5 min cuts commit
# frequency 10×. Tradeoff: silver "old per device" lags bronze by up to 5 min.
SILVER_EVERY_N_BATCHES = 10

# Periodic Iceberg snapshot expiration on the silver tables. Workaround for
# S3 Tables' default maxSnapshotAgeHours=120 (5 days). POC run 000000037h6imf7gsk3
# confirmed that `CALL system.expire_snapshots` errors at SQL parse time on
# this S3 Tables catalog — either IcebergSparkSessionExtensions are not
# applied for the S3TablesCatalog, or S3 Tables blocks system procedures.
# Leaving the codepath in place behind a flag in case AWS Support clarifies.
ENABLE_EXPIRE_SNAPSHOTS  = False
EXPIRE_EVERY_N_SILVERS   = 6       # run after every 6 silver MERGEs (~30 min)
EXPIRE_RETAIN_LAST       = 20      # keep last 20 snapshots — well above what MERGE needs
EXPIRE_OLDER_THAN_HOURS  = 6       # expire anything older than 6h


# ─── Helpers ───────────────────────────────────────────────────────────────

def load_s3_text(path: str) -> str:
    """Load a small text file from S3 via boto3.
    Replaces spark.sparkContext.wholeTextFiles(path).collect() which spins
    up a full Spark job for a 1KB read."""
    assert path.startswith("s3://"), f"Expected s3:// path, got {path}"
    bucket, key = path[5:].split("/", 1)
    s3 = boto3.client("s3", region_name="ap-south-1")
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def avsc_field_names(avsc_text: str) -> list[str]:
    schema = json.loads(avsc_text)
    return [f["name"] for f in schema.get("fields", [])]


# Positive results cached forever; negative results NOT cached (table may
# be created mid-stream and we need to re-check).
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


def get_latest_per_device(df: DataFrame, ts_col: str = "ts") -> DataFrame:
    """Return one row per deviceID — the one with the largest ts_col.

    Equivalent to:
        Window.partitionBy("deviceID").orderBy(col(ts_col).desc())
        row_number() == 1

    But uses a single hash-aggregation pass (groupBy + max of struct)
    instead of a shuffle + sort + window scan. ~2-3× faster on this
    workload. Struct ordering compares field-by-field, so putting ts
    first means the max struct is the row with the old ts.
    """
    other_cols = [c for c in df.columns if c != "deviceID" and c != ts_col]
    ordered = [ts_col] + other_cols
    return (df
        .groupBy("deviceID")
        .agg(F_max(struct(*ordered)).alias("_latest"))
        .select("deviceID", "_latest.*"))


# ─── Main ──────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        _run()
    except Exception:
        # Best-effort stderr trace. Note: K8s SIGTERM can kill the process
        # before Python flushes; the try/except wrapper around
        # query.awaitTermination() below is more reliable for streaming
        # failures specifically.
        print("STREAMING JOB FAILED (top-level):", file=sys.stderr, flush=True)
        traceback.print_exc(file=sys.stderr)
        sys.stderr.flush()
        raise


def _run() -> None:
    # NOTE: most Spark configs come from job-submit-streaming-v3.json
    # (the spark-defaults block). The few configs set here are either:
    #   (a) defensive duplicates that MUST be set before SparkSession starts, or
    #   (b) Spark UI / monitoring retention (small in-memory caches), or
    #   (c) advisory values that get overridden by checkpoint state but kept
    #       for first-run safety.
    spark = (SparkSession.builder
        .appName("iot-streaming-bronze-silver-eks-v3")
        # Streaming state-store maintenance — keep these in script in case
        # JSON overrides are missed; harmless if duplicated.
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "2min")
        .config("spark.sql.streaming.stateStore.timeout", "10min")
        .config("spark.sql.streaming.streamingProgressMaxRetained", "10")
        # Spark UI retention — keep modest to avoid driver memory growth
        # over multi-day streaming runs.
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
        "startingOffsets":         "old",
        "failOnDataLoss":          "false",
        "groupIdPrefix":           CONSUMER_GROUP,
        "kafka.security.protocol": "PLAINTEXT",
        # Raised from 150K — POC saw "falling behind" warnings during initial
        # catch-up. At 4,157 rec/s steady (300M/day) and 30s triggers we need
        # ~125K/batch normally, but bursts to 16.6K rec/s would push that to
        # ~500K. Letting Kafka feed up to 500K per trigger absorbs bursts and
        # speeds recovery after restarts.
        "maxOffsetsPerTrigger":    "500000",
    }

    logger.info(f"Connecting to Kafka topic: {KAFKA_TOPIC}")
    logger.info(f"Checkpoint location: {CHECKPOINT_COMBINED}")
    logger.info(f"maxOffsetsPerTrigger: {kafka_params['maxOffsetsPerTrigger']}")

    raw = spark.readStream.format("kafka").options(**kafka_params).load()

    decoded = raw.select(
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*")

    # ─── Bronze writer ─────────────────────────────────────────────────────
    # Receives the RAW decoded batch. We do `ts` conversion here only — the
    # other epoch fields (originalTs, consumedAt, pushedAt) stay as raw
    # BIGINT, matching the existing bronze Iceberg table schema. Silver
    # does its own timestamp conversion for those fields.
    def write_bronze(batch_df: DataFrame, batch_id: int) -> None:
        bronze_df = (batch_df
            .withColumn("timeLatency",     F_round((col("pushedAt") - col("ts")) / (1000 * 60), 2))
            .withColumn("consumedLatency", F_round((col("consumedAt") - col("pushedAt")) / (1000 * 60), 2))
            .withColumn("ts",              (col("ts") / 1000).cast("timestamp"))
            .withColumn("ts",              from_utc_timestamp("ts", "Asia/Kolkata"))
            .withColumn("insert_date",     to_date("ts"))
            .withColumn("year",            year("ts"))
            .withColumn("month",           month("ts"))
            .withColumn("day",             day("ts"))
            .select(col_order))

        # Cast alarms struct (Avro decoded) → struct of strings with renamed fields.
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
            # Fallback: alarms came through as JSON string
            struct_cols = [
                get_json_object(col("alarms"), f"$.{k}").alias(rename_map[k])
                for k in sorted(expected_fields)
            ]

        out_df = bronze_df.withColumn("alarms", struct(*struct_cols))
        spark.sql("CREATE NAMESPACE IF NOT EXISTS `s3tablescatalog/bs-iot-tables-poc`.bronze")

        # Coalesce to control file count. ~105K rec/batch / 4 files = ~26K rec/file.
        out_df = out_df.coalesce(BRONZE_COALESCE)

        if table_exists(spark, BRONZE_TABLE):
            out_df.writeTo(BRONZE_TABLE).append()
        else:
            (out_df.writeTo(BRONZE_TABLE)
                .partitionedBy("year", "month", "day")
                .tableProperty("format-version", "2")
                # merge-on-read by default — only matters if anyone MERGEs into
                # bronze later; safe and forward-compatible.
                .tableProperty("write.merge.mode", "merge-on-read")
                # Survive S3 Tables auto-maintenance commit races: default
                # 4 retries with 100ms backoff is too tight; bump to 10 retries
                # with 2s–60s exponential backoff over a 30 min total window.
                .tableProperty("commit.retry.num-retries", "10")
                .tableProperty("commit.retry.min-wait-ms", "2000")
                .tableProperty("commit.retry.max-wait-ms", "60000")
                .tableProperty("commit.retry.total-timeout-ms", "1800000")
                .create())

        logger.info(f"[Bronze] Batch {batch_id}: written ({BRONZE_COALESCE} files)")

    # ─── Silver writer ─────────────────────────────────────────────────────
    def write_silver(batch_df: DataFrame, batch_id: int) -> None:
        # Convert epoch-ms fields to IST timestamps for silver consumption.
        # Bronze keeps these as BIGINT (its existing schema); silver wants
        # actual TIMESTAMPs for the MERGE comparisons.
        silver_df = (batch_df
            .withColumn("ts",         (col("ts")         / 1000).cast("timestamp"))
            .withColumn("originalTs", (col("originalTs") / 1000).cast("timestamp"))
            .withColumn("consumedAt", (col("consumedAt") / 1000).cast("timestamp"))
            .withColumn("pushedAt",   (col("pushedAt")   / 1000).cast("timestamp"))
            .withColumn("ts",         from_utc_timestamp("ts",         "Asia/Kolkata"))
            .withColumn("originalTs", from_utc_timestamp("originalTs", "Asia/Kolkata"))
            .withColumn("consumedAt", from_utc_timestamp("consumedAt", "Asia/Kolkata"))
            .withColumn("pushedAt",   from_utc_timestamp("pushedAt",   "Asia/Kolkata")))

        # FAST per-device old — single hash aggregation, no window scan.
        latest_rows_df = get_latest_per_device(silver_df, ts_col="ts")

        # Validity flags
        valid_df = (silver_df
            .withColumn("isBMSValid",
                        F.when((col("voltage") > 0) & (col("voltage") < 70), 1).otherwise(0))
            .withColumn("isIoTValid",
                        F.when(col("lon") > 0, 1).otherwise(0)))

        bms_valid_latest = (
            get_latest_per_device(
                valid_df.filter(col("isBMSValid") == 1)
                        .select("deviceID", "ts", "lat", "lon", "voltage", "soc", "current",
                                "temperature", "cellVolt", "cellTemp", "alarms"),
                ts_col="ts")
            .withColumnsRenamed({"ts": "bmsLastTs", "lat": "bmsLastLat", "lon": "bmsLastLon"})
        )

        iot_valid_latest = (
            get_latest_per_device(
                valid_df.filter(col("isIoTValid") == 1).select("deviceID", "ts", "lat", "lon"),
                ts_col="ts")
            .withColumnRenamed("ts", "iotLastTs")
        )

        latest_keys = latest_rows_df.select("deviceID", "ts")
        combined = (latest_keys
            .join(bms_valid_latest, "deviceID", "left")
            .join(iot_valid_latest, "deviceID", "left")
            .coalesce(SILVER_COALESCE))

        spark.sql("CREATE NAMESPACE IF NOT EXISTS `s3tablescatalog/bs-iot-tables-poc`.silver")

        # ── silver.iot_events_latest_valid ───────────────────────────────
        if not table_exists(spark, SILVER_VALID_TABLE):
            (combined.writeTo(SILVER_VALID_TABLE)
                .tableProperty("format-version", "2")
                .tableProperty("write.merge.mode", "merge-on-read")
                .tableProperty("write.update.mode", "merge-on-read")
                .tableProperty("write.delete.mode", "merge-on-read")
                # Commit retry + snapshot isolation: S3 Tables runs auto
                # compaction/expiration on managed tables; the default
                # `serializable` isolation rejects our MERGE commit when
                # maintenance has changed the version token mid-flight.
                # `snapshot` isolation allows the commit through; the
                # commit.retry.* values give Iceberg room to retry through
                # maintenance windows.
                .tableProperty("commit.retry.num-retries", "10")
                .tableProperty("commit.retry.min-wait-ms", "2000")
                .tableProperty("commit.retry.max-wait-ms", "60000")
                .tableProperty("commit.retry.total-timeout-ms", "1800000")
                .tableProperty("write.merge.isolation-level", "snapshot")
                .create())
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

        # ── silver.iot_events_latest ─────────────────────────────────────
        latest_to_write = latest_rows_df.coalesce(SILVER_COALESCE)
        if not table_exists(spark, SILVER_LATEST_TABLE):
            (latest_to_write.writeTo(SILVER_LATEST_TABLE)
                .tableProperty("format-version", "2")
                .tableProperty("write.merge.mode", "merge-on-read")
                .tableProperty("write.update.mode", "merge-on-read")
                .tableProperty("write.delete.mode", "merge-on-read")
                # Same commit-retry + snapshot-isolation rationale as
                # silver.iot_events_latest_valid above.
                .tableProperty("commit.retry.num-retries", "10")
                .tableProperty("commit.retry.min-wait-ms", "2000")
                .tableProperty("commit.retry.max-wait-ms", "60000")
                .tableProperty("commit.retry.total-timeout-ms", "1800000")
                .tableProperty("write.merge.isolation-level", "snapshot")
                .create())
        else:
            latest_to_write.createOrReplaceGlobalTempView("latest_src")
            spark.sql(f"""
                MERGE INTO {SILVER_LATEST_TABLE} AS target
                USING global_temp.latest_src AS source
                ON target.deviceID = source.deviceID
                WHEN MATCHED AND source.ts > target.ts THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        logger.info(f"[Silver] Batch {batch_id}: upserted")

    # ─── Combined processor ────────────────────────────────────────────────
    def process_combined(batch_df: DataFrame, batch_id: int) -> None:
        # IMPORTANT: bronze schema (already-existing Iceberg table) keeps
        # originalTs/consumedAt/pushedAt as BIGINT (raw epoch ms). Only `ts`
        # is converted to TIMESTAMP. Silver does its own conversion for the
        # other fields inside write_silver. Do NOT pre-convert all timestamps
        # here — it broke the bronze append in job 000000037gnbuinahki.
        #
        # We cache the raw batch_df instead so bronze and silver share the
        # Avro decode work (the most expensive upstream operation).
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            # take(1) — cheap empty check (was: batch_df.rdd.isEmpty() which
            # triggers a full pass)
            if not batch_df.take(1):
                logger.info(f"Batch {batch_id}: empty, skipping.")
                return

            # Bronze first — append-only, source of truth.
            try:
                write_bronze(batch_df, batch_id)
            except Exception as e:
                # Bronze failure is FATAL — don't continue to silver because
                # silver is derivable from bronze but not vice versa.
                logger.error(f"[Bronze] Batch {batch_id} FAILED: {e}")
                raise

            # Silver runs only every Nth batch. Bronze stays real-time; silver
            # is a derived "old per device" view that we cap at ~1 commit per
            # 5 min. Tradeoff: a device whose only update lands in a skipped
            # batch won't be reflected in silver until the next silver tick.
            # That's acceptable because silver is used for current-state lookups
            # at ~5 min freshness, not as a transaction log.
            if batch_id % SILVER_EVERY_N_BATCHES != 0:
                logger.info(f"[Silver] Batch {batch_id}: skipped (cadence 1-in-{SILVER_EVERY_N_BATCHES})")
                return

            # Silver — upserts. Two failure classes:
            #
            # (1) Transient S3 Tables commit conflicts: ConflictException /
            #     CommitFailedException. Caused by S3 Tables auto-maintenance
            #     (compaction, snapshot expiration) racing with our MERGE
            #     commit. After Iceberg's own commit.retry.* exhausts, we
            #     swallow it — silver is derivable from bronze (which we
            #     already wrote above), so a missed batch reconciles itself
            #     on the next batch's MERGE. Better than killing the stream.
            #
            # (2) Anything else: real bug or systemic issue. Re-raise — we
            #     want loud feedback, not silent drift.
            try:
                write_silver(batch_df, batch_id)
            except Exception as e:
                err_str = str(e)
                if "CommitFailedException" in err_str or "ConflictException" in err_str:
                    logger.warning(
                        f"[Silver] Batch {batch_id}: transient commit conflict — "
                        f"skipping batch, bronze already written, "
                        f"silver will reconcile on next MERGE: {e}"
                    )
                else:
                    logger.error(f"[Silver] Batch {batch_id} FAILED (bronze succeeded): {e}")
                    raise

            # Periodic snapshot expiration on silver tables. Workaround for
            # S3 Tables' 120h default snapshot retention.
            if ENABLE_EXPIRE_SNAPSHOTS:
                silver_run_index = batch_id // SILVER_EVERY_N_BATCHES
                if silver_run_index > 0 and silver_run_index % EXPIRE_EVERY_N_SILVERS == 0:
                    for tbl in ("silver.iot_events_latest", "silver.iot_events_latest_valid"):
                        try:
                            spark.sql(f"""
                                CALL `{CATALOG}`.system.expire_snapshots(
                                    table => '{tbl}',
                                    older_than => TIMESTAMPADD(HOUR, -{EXPIRE_OLDER_THAN_HOURS}, current_timestamp()),
                                    retain_last => {EXPIRE_RETAIN_LAST}
                                )
                            """).collect()
                            logger.info(f"[Expire] Batch {batch_id}: expire_snapshots OK on {tbl}")
                        except Exception as e:
                            # Don't kill the stream — if maintenance fails we still
                            # have the auto-managed retention as a backstop.
                            logger.warning(f"[Expire] Batch {batch_id}: {tbl} failed (continuing): {e}")
        finally:
            batch_df.unpersist()

    # ─── Start the stream ──────────────────────────────────────────────────
    logger.info("Starting combined Bronze+Silver stream...")
    query = (decoded.writeStream
        .foreachBatch(process_combined)
        .outputMode("update")  # honest about what foreachBatch is doing
        .option("checkpointLocation", CHECKPOINT_COMBINED)
        .trigger(processingTime="30 seconds")
        .start())

    logger.info("Stream running. Awaiting termination...")

    # Streaming-aware exception handling. query.exception() returns the
    # actual StreamingQueryException with a real stack trace, which is
    # often missing from awaitTermination's bubbled-up exception.
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
