#!/usr/bin/env python3
"""Receive completion messages and download the named results file.

Requires boto3. Configuration: MAXIMIZER_AWS_ACCESS_KEY_ID,
MAXIMIZER_AWS_SECRET_ACCESS_KEY, MAXIMIZER_AWS_REGION,
MAXIMIZER_RESULTS_BUCKET, MAXIMIZER_RESULTS_QUEUE_URL,
and optional MAXIMIZER_DOWNLOAD_DIR (default ./results).
"""
import json
import os
import signal
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = os.environ["MAXIMIZER_AWS_REGION"]  # required: the region in your config file
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
    """Download the results object named in the completion message."""
    key = status["results_file_name"]
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
