## Identifiers you'll need (paste-ready)

| Item | Value |
|---|---|
| AWS profile | `batterysmart` |
| Region | `ap-south-1` |
| Account | `910556394401` |
| Virtual cluster id | `brtw9ztup2y01wok6x8r2bsmr` |
| EKS namespace | `emr-prod` |
| Execution role | `arn:aws:iam::910556394401:role/bs-analytics-emr-on-eks-irsa` |
| Script S3 path | `s3://emr-migration-poc/scripts/streaming_v1_s3tables_eks.py` |
| Job-submit JSON | `C:\Users\91960\Downloads\emr-eks-poc\job-submit-streaming-client-tagged-new.json` |
| CloudWatch log group | `/emr-on-eks/bs-analytics` |
| Log stream prefix | `streaming-bronze-silver` |
| S3 log root | `s3://emr-migration-poc/emr-eks-logs/brtw9ztup2y01wok6x8r2bsmr/jobs/<JOB-ID>/` |

## Submit a job

```
aws emr-containers start-job-run --cli-input-json file://C:\Users\91960\Downloads\emr-eks-poc\job-submit-streaming-client-tagged-new.json --profile batterysmart --region ap-south-1
```

Output gives you the `id` (e.g. `000000037glvqikoonn`). Save it — you'll use it everywhere below.

If you need a new variation, copy the JSON, edit it, submit with `--cli-input-json file://<new-file>`. Keep `cost-center=emr-eks-prod`.

---

## Check job state

```
aws emr-containers describe-job-run --virtual-cluster-id brtw9ztup2y01wok6x8r2bsmr --id <JOB-ID> --profile batterysmart --region ap-south-1 --query "jobRun.{State:state,StateDetails:stateDetails,CreatedAt:createdAt,FinishedAt:finishedAt,Failure:failureReason}" --output table
```

States you'll see: `SUBMITTED` → `PENDING` → `RUNNING` → (`COMPLETED` | `FAILED` | `CANCELLED`). Transition from SUBMITTED to RUNNING takes ~1–2 min.

---

## Tail batch logs (Python `[Bronze] Batch N` / `[Silver] Batch N` lines)

**Use PowerShell, not Bash on Windows** — Git Bash mangles `/emr-on-eks/...` paths:

```
aws logs tail /emr-on-eks/bs-analytics --log-stream-name-prefix streaming-bronze-silver --since 10m --filter-pattern "Batch" --profile batterysmart --region ap-south-1
```

If you want all driver output (chatty unless `spark.log.level=WARN` is set in the JSON):

```
aws logs tail /emr-on-eks/bs-analytics --log-stream-name-prefix streaming-bronze-silver --since 10m --follow --profile batterysmart --region ap-south-1
```

---

## Cancel a running job

```
aws emr-containers cancel-job-run --virtual-cluster-id brtw9ztup2y01wok6x8r2bsmr --id <JOB-ID> --profile batterysmart --region ap-south-1
```

---

## When a job fails — where to look

EMR's `stateDetails` field is usually too generic. The real cause lives in the per-container logs on S3:

```
aws s3 ls s3://emr-migration-poc/emr-eks-logs/brtw9ztup2y01wok6x8r2bsmr/jobs/<JOB-ID>/ --recursive --profile batterysmart --region ap-south-1
```

You'll see:
- `containers/spark-<job-id>/spark-<job-id>-driver/stderr.gz` — Java/Spark driver log
- `containers/spark-<job-id>/spark-<job-id>-driver/stdout.gz` — Python `logger.info` lines from the script
- `containers/spark-<job-id>/iot-streaming-bronze-silver-eks-...-exec-N/stderr.gz` — per-executor logs
- `control-logs/<job-id>-<suffix>/stderr.gz` — **read this first.** Shows container exit codes + `termination reason` (OOMKilled, Completed, Error, etc.) for both the spark-kubernetes-driver and emr-container-fluentd sidecars.

Download with:
```
aws s3 cp <s3-path> <local-path>.gz --profile batterysmart --region ap-south-1
```

Decompress on Windows (PowerShell):
```
$in=".\file.gz"; $out=".\file"; $fs=[IO.File]::OpenRead($in); $gz=New-Object IO.Compression.GZipStream($fs,[IO.Compression.CompressionMode]::Decompress); $os=[IO.File]::Create($out); $gz.CopyTo($os); $os.Close(); $gz.Close(); $fs.Close()
```

Common exit codes:
- `0` / `Completed` — clean shutdown
- `137` / `OOMKilled` — container exceeded its memory limit (fluentd or spark)
- `143` / `Error` — container received SIGTERM (K8s killed the pod — node drain, spot interruption, autoscaler, etc.)
- `11` / `Error` — Spark driver application exit (Python main raised an exception; check driver stderr/stdout for the traceback)

---

