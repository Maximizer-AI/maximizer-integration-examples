# Maximizer AI — Integration Examples

Reference client code for integrating with the Maximizer AI post-call analysis
pipeline. The pipeline is **object-store-to-object-store and event-driven**: you
place a call recording in an **input** location, the pipeline processes it
(speech-to-text → analysis), and you read a JSON result from an **output**
location. Two clouds are supported; pick the one your tenant runs on.

> All examples read credentials and endpoints from **environment variables** —
> never hard-code them. The values (bucket names, region, keys) are provided by
> Maximizer during onboarding.

## Layout

```
aws/        Amazon S3 integration (SNS→SQS notification, bucket poll fallback)
  python/upload_and_fetch.py        boto3 (one-shot: upload → notification → download)
  python/consumer.py                boto3 (long-running consumer, drop-in for the GCP consumer)
  javascript/upload_and_fetch.mjs   @aws-sdk/client-s3 v3
  java/UploadAndFetch.java          AWS SDK for Java v2
gcp/        Google Cloud integration (Pub/Sub notification + GCS)
  python/consumer.py                google-cloud-pubsub / -storage
  javascript/consumer.js            @google-cloud/pubsub / storage
  java/Consumer.java                google-cloud-pubsub / -storage
samples/    Example result payload (synthetic demo call)
```

## AWS model (S3)

1. **Upload** the recording to the input bucket under an agreed category prefix
   (`claims/`, `sales/`, `new_policy_sales/`, `renew_policy_sales/`), e.g.
   `claims/DEMO-CALL-0001.wav`.
2. The pipeline delivers a **curated results file** to the results location and
   notifies you. This file is a display document built **only** from the fields
   configured for your tenant (an allow-list); its format is `.json` or
   `.pdf` per your tenant configuration.
3. **Get notified and retrieve.** For every processed file the pipeline
   publishes one `file_status` message — `{"upload_file_name", "results_file_name",
   "status": "processed", "message_timestamp"}` — to your environment's
   notification queue (SNS → SQS). You receive it with the same access key, from
   your registered IP ranges only; delete the message once handled. The examples
   use the queue when `MAXIMIZER_RESULTS_QUEUE_URL` is set and fall back to
   polling the results bucket under your category prefix otherwise.

> **You never receive `*.llm-results.json`.** That is Maximizer's internal
> full-pipeline dataset. The client always receives the curated display file.

Encryption (SSE-KMS) and TLS are automatic. Access is via a least-privilege IAM
key that is **fail-closed on IP** — it works only from source ranges you
register with Maximizer. Do not name uploads ending in the reserved suffixes
`_analyze_text_task_data.json` / `_analyze_call_task_data.json`.

```bash
export MAXIMIZER_AWS_ACCESS_KEY_ID=...      MAXIMIZER_AWS_SECRET_ACCESS_KEY=...
export MAXIMIZER_AWS_REGION=us-west-2
export MAXIMIZER_INPUT_BUCKET=your-input-bucket
export MAXIMIZER_RESULTS_BUCKET=your-results-bucket
export MAXIMIZER_RESULTS_EXT=json           # or: pdf
export MAXIMIZER_RESULTS_QUEUE_URL=...      # your notification queue (from onboarding)
python3 aws/python/upload_and_fetch.py claims ./call.wav
```

## GCP model (Pub/Sub + GCS)

Upload the recording to the GCS upload bucket under a category prefix. When the
result is ready the pipeline publishes a Pub/Sub message; the consumer examples
subscribe to it and download the results object from GCS.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
export MAXIMIZER_GCP_PROJECT_ID=your-gcp-project-id
export MAXIMIZER_GCP_SUBSCRIPTION=your-results-subscription
export MAXIMIZER_RESULTS_BUCKET=your-results-bucket
python3 gcp/python/consumer.py
```

## Notes

- The result JSON schema is identical across clouds — see `samples/`.
- These are minimal, dependency-light references, not production clients; add
  your own ret/observability, idempotency and error handling.
