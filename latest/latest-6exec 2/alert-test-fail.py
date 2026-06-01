# Throwaway job to validate EMR-on-EKS -> EventBridge -> SNS email alerting.
# Fails immediately (BEFORE creating any SparkSession), so it touches no data,
# no Kafka, no S3 Tables. With maxAttempts=2 the run retries once, which
# exercises BOTH the FAILED state-change event and the per-retry driver event.
import sys

print("alert-test: starting; about to fail intentionally", flush=True)
raise RuntimeError("intentional test failure to validate EMR-on-EKS alert email path")
