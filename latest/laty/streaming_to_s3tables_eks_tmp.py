#!/usr/bin/env python3
"""
IoT Streaming Pipeline — V2 — EMR on EKS — Bronze + Silver (inline Athena reconcile)

SILVER APPROACH (this variant):
  Bronze is UNCHANGED.
  Everything happens inside ONE 30s micro-batch (foreachBatch, on the driver):
    1. DROP the two silver _tmp tables (discards prior batch + its snapshots).
    2. RECREATE + LOAD this batch's "latest per device" rows into the _tmp
       tables (Spark append/create — cheap). Because each batch is already
       aggregated to one row per device, _tmp holds exactly one row per device.
    3. Call ATHENA (boto3, from the driver) to MERGE _tmp -> final silver
       tables. The two MERGEs run in parallel. No MERGE in Spark SQL.

  The final silver tables are created EMPTY (correct bucketed schema) on the
  first batch; Athena fills them thereafter.

  *** OPERATIONAL WARNING ***
  The Athena MERGE runs synchronously inside the batch and BLOCKS it. As the
  final tables grow, MERGE latency grows. If the two MERGEs exceed the 30s
  trigger, batches will back up. Watch the BatchDurationMs CloudWatch metric.
  Mitigations already applied: the two MERGEs run concurrently. If you still
  lag, raise the trigger interval or move the reconcile back out to a scheduled
  job.

  IAM: the job execution role now also needs:
    athena:StartQueryExecution, athena:GetQueryExecution, athena:GetQueryResults,
    athena:StopQueryExecution, s3:* on the Athena output prefix, and Lake
    Formation / S3 Tables permissions to MERGE/SELECT/DELETE on the silver
    tables and read the _tmp tables.

Original V2 notes (unchanged): repartition(SILVER_BUCKETS,"deviceID"),
maxOffsetsPerTrigger 150_000, MEMORY_AND_DISK persist, Iceberg auto-compaction
table props, best-effort startup rewrite_data_files.
"""

from __future__ import annotations

import json
import logging
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ─── Config ────────────────────────────────────────────────────────────────
S3_BUCKET           = "emr-migration-poc"
KAFKA_BROKERS       = ("b-1.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-2.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092,"
                      "b-3.mskinternalprodcluste.3mco1i.c4.kafka.ap-south-1.amazonaws.com:9092")
KAFKA_TOPIC         = "normalized-iot-events"
CONSUMER_GROUP      = "emr_eks_streaming_v2"

BASE                = f"s3://{S3_BUCKET}"
CHECKPOINT_COMBINED = f"{BASE}/checkpoints/eks/combined/iot_streaming_final_v3"
SCHEMA_PATH         = f"{BASE}/config/schemav1.avsc"
RENAME_MAP_PATH     = f"{BASE}/config/rename_mapv1.json"

CATALOG             = "s3tablescatalog/bs-iot-tables-poc"
BRONZE_TABLE        = f"`{CATALOG}`.bronze.iot_v3"

# Final silver tables (written ONLY by the inline Athena MERGE).
SILVER_LATEST_TABLE = f"`{CATALOG}`.silver.iot_events_latest_v3"
SILVER_VALID_TABLE  = f"`{CATALOG}`.silver.iot_events_latest_valid_v3"

# Intermediate staging tables (dropped + recreated every batch).
SILVER_LATEST_TMP_TABLE = f"`{CATALOG}`.silver.iot_events_latest_v3_tmp"
SILVER_VALID_TMP_TABLE  = f"`{CATALOG}`.silver.iot_events_latest_valid_v3_tmp"

# Short (unqualified) names used inside Athena SQL.
LATEST_FINAL_SHORT = "iot_events_latest_v3"
VALID_FINAL_SHORT  = "iot_events_latest_valid_v3"
LATEST_TMP_SHORT   = "iot_events_latest_v3_tmp"
VALID_TMP_SHORT    = "iot_events_latest_valid_v3_tmp"

REGION              = "ap-south-1"
CW_NAMESPACE        = "BatterySmart/IoTStreaming/V2"

# ─── Athena config — CONFIRM against your S3 Tables / Lake Formation wiring ──
# ATHENA_CATALOG must be the Athena DataCatalog name that exposes the S3 Tables
# catalog. Depending on how it was registered this may be a federated catalog
# name (often "s3tablescatalog/bs-iot-tables-poc") or "AwsDataCatalog". Adjust
# ATHENA_DATABASE if your silver namespace is surfaced under a different db.
ATHENA_CATALOG      = "s3tablescatalog/bs-iot-tables-poc"
ATHENA_DATABASE     = "silver"
ATHENA_WORKGROUP    = "primary"
ATHENA_OUTPUT       = f"{BASE}/athena/silver-reconcile/"
ATHENA_POLL_S       = 1.0
ATHENA_TIMEOUT_S    = 120

BRONZE_COALESCE = 4
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


# ─── Athena reconciler (runs MERGE on the driver) ──────────────────────────

class AthenaReconciler:
    def __init__(self, region, workgroup, output, catalog, database,
                 poll_s=ATHENA_POLL_S, timeout_s=ATHENA_TIMEOUT_S):
        self.client    = boto3.client("athena", region_name=region)
        self.workgroup = workgroup
        self.output    = output
        self.catalog   = catalog
        self.database  = database
        self.poll_s    = poll_s
        self.timeout_s = timeout_s

    def run(self, sql: str) -> None:
        resp = self.client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Catalog": self.catalog, "Database": self.database},
            WorkGroup=self.workgroup,
            ResultConfiguration={"OutputLocation": self.output},
        )
        qid = resp["QueryExecutionId"]
        deadline = time.time() + self.timeout_s
        while True:
            st = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            state = st["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() > deadline:
                try:
                    self.client.stop_query_execution(QueryExecutionId=qid)
                except Exception as e:
                    raise TimeoutError(f"Athena query stop_query_execution {e}")
                raise TimeoutError(f"Athena query {qid} exceeded {self.timeout_s}s")
            time.sleep(self.poll_s)
        if state != "SUCCEEDED":
            reason = st.get("StateChangeReason", "")
            raise RuntimeError(f"Athena query {state}: {reason}\nSQL:\n{sql}")

    def run_parallel(self, sqls: list[str]) -> None:
        """Run independent MERGEs concurrently; raise if any fails."""
        with ThreadPoolExecutor(max_workers=len(sqls)) as ex:
            futures = [ex.submit(self.run, s) for s in sqls]
            errors = []
            for fut in futures:
                try:
                    fut.result()
                except Exception as e:
                    errors.append(e)
        if errors:
            raise errors[0]


def build_latest_merge(final: str, tmp: str, cols: list[str]) -> str:
    """latest table: source already one row per device; replace row when newer.
    All identifiers quoted to preserve camelCase column names."""
    data_cols  = [c for c in cols if c != "deviceID"]
    sel        = ", ".join(f'"{c}"' for c in cols)
    set_clause = ", ".join(f'"{c}" = s."{c}"' for c in data_cols)
    ins_cols   = ", ".join(f'"{c}"' for c in cols)
    ins_vals   = ", ".join(f's."{c}"' for c in cols)
    return f'''
        MERGE INTO "{final}" AS t
        USING (SELECT {sel} FROM "{tmp}") AS s
        ON t."deviceID" = s."deviceID"
        WHEN MATCHED AND s."ts" > t."ts" THEN UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        '''


def build_valid_merge(final: str, tmp: str) -> str:
    """valid table: independent BMS / IoT timestamp gating against the target.
    Source is one row per device (single batch), so no windowing needed."""
    bms_gate = '(t."bmsLastTs" IS NULL OR s."bmsLastTs" > t."bmsLastTs")'
    iot_gate = '(t."iotLastTs" IS NULL OR s."iotLastTs" > t."iotLastTs")'

    def g(field, gate):
        return f'"{field}" = CASE WHEN {gate} THEN s."{field}" ELSE t."{field}" END'

    bms_fields = ["bmsLastTs", "bmsLastLat", "bmsLastLon", "voltage", "soc",
                  "current", "temperature", "cellVolt", "cellTemp", "alarms"]
    iot_fields = ["iotLastTs", "lat", "lon"]

    sets  = ['"ts" = CASE WHEN t."ts" IS NULL OR s."ts" > t."ts" THEN s."ts" ELSE t."ts" END']
    sets += [g(f, bms_gate) for f in bms_fields]
    sets += [g(f, iot_gate) for f in iot_fields]
    set_clause = ",\n    ".join(sets)

    all_cols = ["deviceID", "ts"] + bms_fields + iot_fields
    ins_cols = ", ".join(f'"{c}"' for c in all_cols)
    ins_vals = ", ".join(f's."{c}"' for c in all_cols)
    sel      = ", ".join(f'"{c}"' for c in all_cols)
    return f'''
        MERGE INTO "{final}" AS t
        USING (SELECT {sel} FROM "{tmp}") AS s
        ON t."deviceID" = s."deviceID"
        WHEN MATCHED THEN UPDATE SET
            {set_clause}
        WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})
        '''


# ─── CloudWatch progress listener ──────────────────────────────────────────

class ProgressListener(StreamingQueryListener):
    """Publishes per-batch metrics to CloudWatch and warns on sustained lag."""

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
                    f"trigger={trigger_ms}ms). If trigger is high, the inline Athena "
                    f"MERGE is likely the bottleneck."
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
    return (batch_df
        .withColumn("timeLatency",     F_round((col("pushedAt")   - col("ts"))       / (1000 * 60), 2))
        .withColumn("consumedLatency", F_round((col("consumedAt") - col("pushedAt")) / (1000 * 60), 2))
        .withColumn("ts",         from_utc_timestamp((col("ts")         / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("originalTs", from_utc_timestamp((col("originalTs") / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("consumedAt", from_utc_timestamp((col("consumedAt") / 1000).cast("timestamp"), "Asia/Kolkata"))
        .withColumn("pushedAt",   from_utc_timestamp((col("pushedAt")   / 1000).cast("timestamp"), "Asia/Kolkata")))


# ─── One-time pre-stream compaction (final tables) ─────────────────────────

def one_time_compact_silver(spark: SparkSession) -> None:
    for short_name in ("silver.iot_events_latest_v3", "silver.iot_events_latest_valid_v3"):
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
        .appName("iot-streaming-v2")
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
    logger.info(f"Bronze table: {BRONZE_TABLE}")
    logger.info(f"Silver staging (drop+recreate per batch): {SILVER_LATEST_TMP_TABLE}, {SILVER_VALID_TMP_TABLE}")
    logger.info(f"Silver final (inline Athena MERGE target): {SILVER_LATEST_TABLE}, {SILVER_VALID_TABLE}")

    reconciler = AthenaReconciler(REGION, ATHENA_WORKGROUP, ATHENA_OUTPUT,
                                  ATHENA_CATALOG, ATHENA_DATABASE)

    one_time_compact_silver(spark)
    spark.streams.addListener(ProgressListener(CW_NAMESPACE, REGION))

    raw = spark.readStream.format("kafka").options(**kafka_params).load()

    decoded = raw.select(
        from_avro(col("value"), avro_schema, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*")

    common_table_props = {
        "format-version":                          "2",
        "write.merge.mode":                        "merge-on-read",
        "write.update.mode":                       "merge-on-read",
        "write.delete.mode":                       "merge-on-read",
        "write.merge.isolation-level":             "snapshot",
        "commit.retry.num-retries":                "5",
        "commit.retry.min-wait-ms":                "500",
        "commit.retry.max-wait-ms":                "5000",
        "commit.retry.total-timeout-ms":           "15000",
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

    # ─── Bronze writer (UNCHANGED) ─────────────────────────────────────────
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

    # ─── Silver: drop+recreate _tmp, load, then inline Athena MERGE ────────
    def recreate_and_load_tmp(stage_df: DataFrame, tmp_table: str) -> None:
        """DROP the _tmp table (discards prior batch + its snapshots), then
        recreate it loaded with this batch's rows."""
        spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")
        _table_exists_cache.pop(tmp_table, None)
        try:
            apply_table_props(stage_df.writeTo(tmp_table)).create()
        except Exception:
            # Fallback if the drop hasn't fully propagated in the catalog.
            stage_df.writeTo(tmp_table).createOrReplace()

    def write_silver(prepared_df: DataFrame, batch_id: int) -> None:
        all_cols = [c for c in prepared_df.columns if c not in ("deviceID", "ts")]
        bms_cols = ["lat", "lon", "voltage", "soc", "current",
                    "temperature", "cellVolt", "cellTemp", "alarms"]
        iot_cols = ["lat", "lon"]

        validity_df = (prepared_df
            .withColumn("isBMSValid",
                        F.when((col("voltage") > 0) & (col("voltage") < 70), 1).otherwise(0))
            .withColumn("isIoTValid",
                        F.when(col("lon") > 0, 1).otherwise(0)))

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
            latest_rows_df = agg.select("deviceID", "_latest.*")

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
            )

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.silver")

            # Ensure final tables exist (EMPTY) — Athena MERGE needs a target.
            if not table_exists(spark, SILVER_LATEST_TABLE):
                apply_table_props(
                    latest_rows_df.limit(0).writeTo(SILVER_LATEST_TABLE)
                                  .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
                logger.info(f"[Silver] Created empty final table {SILVER_LATEST_TABLE}")
            if not table_exists(spark, SILVER_VALID_TABLE):
                apply_table_props(
                    combined.limit(0).writeTo(SILVER_VALID_TABLE)
                            .partitionedBy(bucket(SILVER_BUCKETS, col("deviceID")))
                ).create()
                logger.info(f"[Silver] Created empty final table {SILVER_VALID_TABLE}")

            # 1+2) DROP, RECREATE, LOAD this batch into _tmp (one row per device).
            latest_stage = latest_rows_df.repartition(SILVER_BUCKETS, F.col("deviceID"))
            valid_stage  = combined.repartition(SILVER_BUCKETS, F.col("deviceID"))
            recreate_and_load_tmp(latest_stage, SILVER_LATEST_TMP_TABLE)
            recreate_and_load_tmp(valid_stage,  SILVER_VALID_TMP_TABLE)

            # 3) Inline Athena MERGE _tmp -> final (both run concurrently).
            latest_sql = build_latest_merge(LATEST_FINAL_SHORT, LATEST_TMP_SHORT,
                                            latest_rows_df.columns)
            valid_sql  = build_valid_merge(VALID_FINAL_SHORT, VALID_TMP_SHORT)
            reconciler.run_parallel([latest_sql, valid_sql])

            logger.info(f"[Silver] Batch {batch_id}: _tmp recreated + Athena MERGE done")
        finally:
            agg.unpersist()

    # ─── Combined processor ────────────────────────────────────────────────
    silver_failure_state = {"consecutive": 0}

    def process_combined(batch_df: DataFrame, batch_id: int) -> None:
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
                    f"[Silver] Batch {batch_id}: reconcile failed ({fails}/{SILVER_MAX_CONSECUTIVE_FAILURES}) "
                    f"— bronze written; latest-state self-heals on next report: {e}"
                )
        finally:
            prepared.unpersist()

    # ─── Start the stream ──────────────────────────────────────────────────
    logger.info("Starting combined Bronze+Silver V2 stream (inline Athena reconcile)...")
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