#!/usr/bin/env python3
"""
Maximizer AI — AWS results consumer (Python / boto3).

This is the AWS equivalent of the GCP `consumer.py` you run today: a
long-running process that waits for `file_status` notifications and downloads
each curated results document as soon as it is ready.

GCP (before)                                  AWS (now)
-----------------------------------------     ------------------------------------------
Pub/Sub subscription on                       SQS queue MAXIMIZER_RESULTS_QUEUE_URL
  client_integration_topic                      (fed by an SNS topic, one per environment)
message.ack()                                 sqs.delete_message(...)
GCS results bucket, <category>/<name>.json    S3 results bucket, <category>/<name>.json
service-account JSON key                      IAM access key (works only from your registered IP ranges)

The message payload is IDENTICAL on both clouds:
  {"upload_file_name": "claims/<name>.wav",
   "results_file_name": "claims/<name>.json",
   "status": "processed",
   "message_timestamp": "2026-01-01T12:00:00Z"}

Environment (values provided at onboarding):
  MAXIMIZER_AWS_ACCESS_KEY_ID, MAXIMIZER_AWS_SECRET_ACCESS_KEY, MAXIMIZER_AWS_REGION,
  MAXIMIZER_RESULTS_BUCKET, MAXIMIZER_RESULTS_QUEUE_URL, MAXIMIZER_DOWNLOAD_DIR (optional)

Run:  python3 consumer.py          (Ctrl-C to stop)
Requires: pip install boto3
"""
import json
import os
import signal
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = os.environ.get("MAXIMIZER_AWS_REGION", "us-west-2")
RESULTS_BUCKET = os.environ["MAXIMIZER_RESULTS_BUCKET"]
QUEUE_URL = os.environ["MAXIMIZER_RESULTS_QUEUE_URL"]
DOWNLOAD_DIR = os.environ.get("MAXIMIZER_DOWNLOAD_DIR", "./results")

_creds = dict(
    aws_access_key_id=os.environ["MAXIMIZER_AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["MAXIMIZER_AWS_SECRET_ACCESS_KEY"],
    region_name=REGION,
    config=Config(retries={"max_attempts": 5, "mode": "standard"}),
)
s3 = boto3.client("s3", **_creds)
sqs = boto3.client("sqs", **_creds)

_running = True


def _stop(*_):
    global _running
    _running = False


def handle_result(status: dict) -> None:
    """Download the curated results document named in the notification.

    Replace the body of this function with your own persistence (database,
    CRM, file share). It is the equivalent of `check_and_download_results`
    in the GCP consumer.
    """
    key = status["results_file_name"]                       # e.g. claims/DEMO-CALL-0001.json
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    local_path = os.path.join(DOWNLOAD_DIR, key.replace("/", "__"))
    s3.download_file(RESULTS_BUCKET, key, local_path)
    print(f"[{status['message_timestamp']}] {status['upload_file_name']} -> {local_path}")


def listen_for_messages() -> None:
    """Long-poll the queue; delete each message only after it was handled."""
    print(f"Listening on {QUEUE_URL} (results bucket {RESULTS_BUCKET}) ...")
    while _running:
        try:
            resp = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=10,
                                       WaitTimeSeconds=20, VisibilityTimeout=120)
        except ClientError as e:
            print(f"receive failed: {e}", file=sys.stderr)   # e.g. wrong IP range → AccessDenied
            time.sleep(10)
            continue
        for msg in resp.get("Messages", []):
            try:
                body = json.loads(msg["Body"])
                if body.get("status") == "processed" and "results_file_name" in body:
                    handle_result(body)
                else:
                    print(f"ignoring message: {body}")
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
            except Exception as e:                            # not deleted → redelivered after the visibility timeout
                print(f"failed to handle {msg.get('MessageId')}: {e}", file=sys.stderr)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    listen_for_messages()
