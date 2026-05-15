#!/usr/bin/env python3
"""
POC — Can we CALL Iceberg `system.expire_snapshots` on a table that lives in
an S3 Tables managed bucket?

SAFE: retain_last = 999999. No snapshot will be expired on the real table.
We only want to know: does the procedure call get accepted and return without
erroring? If yes, we can bake it into the streaming job with a smaller
retain_last there.
"""

import sys
import traceback

from pyspark.sql import SparkSession


CATALOG = "s3tablescatalog/bs-iot-tables-poc"


def banner(msg):
    print(f"\n=== {msg} ===", flush=True)


def main():
    spark = (SparkSession.builder
        .appName("poc-expire-snapshots")
        .getOrCreate())

    overall_ok = True

    # (1) Catalog reachable?
    banner("(1) SHOW TABLES IN silver")
    try:
        spark.sql(f"SHOW TABLES IN `{CATALOG}`.silver").show(truncate=False)
        print("STEP 1 OK: catalog/namespace reachable", flush=True)
    except Exception:
        print("STEP 1 FAIL:", flush=True)
        traceback.print_exc()
        overall_ok = False

    # (2) Procedure call — no-op (retain_last is intentionally huge)
    for table in ("silver.iot_events_latest", "silver.iot_events_latest_valid"):
        banner(f"(2) CALL system.expire_snapshots on {table} (retain_last=999999 → no-op)")
        try:
            df = spark.sql(f"""
                CALL `{CATALOG}`.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMPADD(HOUR, -1, current_timestamp()),
                    retain_last => 999999
                )
            """)
            df.show(truncate=False)
            print(f"STEP 2 OK on {table}: procedure accepted and returned", flush=True)
        except Exception:
            print(f"STEP 2 FAIL on {table}:", flush=True)
            traceback.print_exc()
            overall_ok = False

    banner("VERDICT")
    print("POC PASSED — can use expire_snapshots in streaming job"
          if overall_ok else
          "POC FAILED — do NOT enable expire_snapshots in streaming job", flush=True)
    spark.stop()
    sys.exit(0 if overall_ok else 1)


if __name__ == "__main__":
    main()
