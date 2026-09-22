#!/usr/bin/env python3
"""
Maximizer AI — AWS S3 integration example (Python / boto3).

Uploads a call recording to the Maximizer INPUT bucket, then retrieves the
**client results file** from the RESULTS location.

IMPORTANT — what you receive:
The results file is a *curated display document* built only from the fields
configured for your tenant (an allow-list); its format is `.json` or
`.pdf` depending on your tenant configuration. It is NOT the internal
`*.llm-results.json` dataset — clients never receive that.

In production the pipeline notifies you (message queue) with the exact results
key; this example polls the results location under your category prefix as a
simple, dependency-free fallback.

Environment:
    export MAXIMIZER_AWS_ACCESS_KEY_ID=...
    export MAXIMIZER_AWS_SECRET_ACCESS_KEY=...
    export MAXIMIZER_AWS_REGION=us-west-2
    export MAXIMIZER_INPUT_BUCKET=your-input-bucket
    export MAXIMIZER_RESULTS_BUCKET=your-results-bucket
    export MAXIMIZER_RESULTS_EXT=json          # or: pdf
    export MAXIMIZER_RESULTS_QUEUE_URL=...       # notification queue (recommended); omit to poll the bucket

    python3 upload_and_fetch.py claims ./call.wav

Requires: pip install boto3
"""
import json
import os
import sys
import time
import boto3
from botocore.config import Config

# The four the pipeline's own resolver recognises (conversation_pipeline.
# get_file_path_pattern). There is no "sales": a file uploaded under it is
# accepted by the bucket, processed, and written where this credential cannot
# read it — so the failure shows up as a missing result, not an upload error.
ALLOWED_PREFIXES = ("claims", "service", "new_policy_sales", "renew_policy_sales")

# The bucket matches the extension case-sensitively and lists only the
# all-lower and all-upper spellings, so ".wav" and ".WAV" are accepted while
# ".Wav" is refused. Mirror that exactly — a case-insensitive check here would
# pass a file the bucket then rejects, which is the confusion this avoids.
ALLOWED_UPLOAD_EXTENSIONS = (".wav", ".mp3", ".json")


def _extension_is_accepted(path: str) -> bool:
    ext = os.path.splitext(path)[1]
    return (ext.lower() in ALLOWED_UPLOAD_EXTENSIONS
            and ext in (ext.lower(), ext.upper()))



def main(category: str, audio_path: str) -> None:
    if category not in ALLOWED_PREFIXES:
        sys.exit(f"category must be one of {ALLOWED_PREFIXES}")
    if not _extension_is_accepted(audio_path):
        sys.exit(f"{audio_path}: only {', '.join(ALLOWED_UPLOAD_EXTENSIONS)} may be "
                 f"uploaded, spelled all-lower or all-upper; convert or rename first")

    region = os.environ.get("MAXIMIZER_AWS_REGION", "us-west-2")
    input_bucket = os.environ["MAXIMIZER_INPUT_BUCKET"]
    results_bucket = os.environ["MAXIMIZER_RESULTS_BUCKET"]
    ext = os.environ.get("MAXIMIZER_RESULTS_EXT", "json").lstrip(".")

    s3 = boto3.client(
        "s3", region_name=region,
        aws_access_key_id=os.environ["MAXIMIZER_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["MAXIMIZER_AWS_SECRET_ACCESS_KEY"],
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )

    # The category comes from the FILE NAME, not the folder. A recording whose
    # name carries no `<category>__` token is analysed as a claim wherever it is
    # put, so the token is what routes the call and the folder is only for
    # organization.
    filename = os.path.basename(audio_path)
    if not filename.startswith(f"{category}__"):
        filename = f"{category}__{filename}"
    key = f"{category}/{filename}"

    # 1) Upload the recording. SSE-KMS and multipart are automatic.
    print(f"uploading {audio_path} -> s3://{input_bucket}/{key}")
    s3.upload_file(audio_path, input_bucket, key)
    print("upload complete")

    # 2) Retrieve the curated results file.
    #    Preferred: wait for the notification. For every processed file the
    #    pipeline publishes one `file_status` message to your environment's
    #    queue: {"upload_file_name", "results_file_name", "status": "processed",
    #    "message_timestamp"}. You receive it with the SAME access key, from
    #    your registered IP ranges only. Delete the message once handled.
    queue_url = os.environ.get("MAXIMIZER_RESULTS_QUEUE_URL")
    deadline = time.time() + 15 * 60
    if queue_url:
        sqs = boto3.client("sqs", region_name=region,
                           aws_access_key_id=os.environ["MAXIMIZER_AWS_ACCESS_KEY_ID"],
                           aws_secret_access_key=os.environ["MAXIMIZER_AWS_SECRET_ACCESS_KEY"])
        print(f"waiting for the file_status notification on {queue_url}")
        while time.time() < deadline:
            resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
            for msg in resp.get("Messages", []):
                status = json.loads(msg["Body"])
                if status.get("upload_file_name") != key:
                    continue  # another file's notification: leave it for its consumer
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                result_key = status["results_file_name"]
                out_path = os.path.basename(result_key)
                s3.download_file(results_bucket, result_key, out_path)
                print(f"result ready ({status['status']} at {status['message_timestamp']}): "
                      f"s3://{results_bucket}/{result_key} -> {out_path}")
                return
        sys.exit("timed out waiting for the notification — contact Maximizer with the upload key")

    #    Fallback without a queue: poll the results location under your category
    #    prefix for a new document.
    # The result is delivered to <category>/<stem>.json, where the stem is the
    # file name with the category token removed.
    result_prefix = f"{category}/"
    seen_before = _keys(s3, results_bucket, result_prefix, ext)
    print(f"waiting for a new *.{ext} result under s3://{results_bucket}/{result_prefix}")
    while time.time() < deadline:
        current = _keys(s3, results_bucket, result_prefix, ext)
        new_keys = [k for k in current if k not in seen_before]
        if new_keys:
            result_key = sorted(new_keys)[-1]
            out_path = os.path.basename(result_key)
            s3.download_file(results_bucket, result_key, out_path)
            print(f"result ready: s3://{results_bucket}/{result_key} -> {out_path}")
            return
        time.sleep(15)
    sys.exit("timed out waiting for result — contact Maximizer with the upload key")


def _keys(s3, bucket, prefix, ext):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return {o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(f".{ext}")}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: upload_and_fetch.py <category> <audio_file>")
    main(sys.argv[1], sys.argv[2])
