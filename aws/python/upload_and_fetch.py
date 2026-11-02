#!/usr/bin/env python3
"""Upload recordings and metadata, then download the completion result.

Configuration and CLI examples are in README.md. Requires boto3.
"""
import json
import math
import os
import re
import sys
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ALLOWED_UPLOAD_EXTENSIONS = (".wav", ".mp3", ".json")
BATCH_SUFFIX = "_batch"
ORG_NAME_RE = re.compile(r"(org|users)_(\d{4}-\d{2}-\d{2})\.json")


def check_category(category: str) -> None:
    if not re.fullmatch(r"[a-z0-9_]+", category) or "__" in category:
        sys.exit("category must use lowercase letters, digits or underscores, with no '__'")


def _extension_is_accepted(path: str) -> bool:
    ext = os.path.splitext(path)[1]
    return ext.lower() in ALLOWED_UPLOAD_EXTENSIONS and ext in (ext.lower(), ext.upper())


def metadata_name(category: str, ucid: str, path: str) -> str:
    prefix = f"{category}__{ucid}"
    name = os.path.basename(path)
    if re.fullmatch(re.escape(prefix) + r"\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.json", name):
        return name
    suffix = os.environ.get("MAXIMIZER_METADATA_SUFFIX", "")
    if not re.fullmatch(r"\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*\.json", suffix):
        sys.exit("metadata filename must be <category>__<UCID>.<suffix>.json, "
                 "or set MAXIMIZER_METADATA_SUFFIX to the full ending agreed at onboarding "
                 "(including the leading dot and .json)")
    return prefix + suffix


def read_json(path: str, limit=None):
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError(f"duplicate JSON key: {key}")
            obj[key] = value
        return obj

    def reject(value):
        raise ValueError(f"non-finite JSON number: {value}")

    def number(value):
        parsed = float(value)
        if not math.isfinite(parsed):
            reject(value)
        return parsed

    try:
        with open(path, "rb") as stream:
            raw = stream.read(limit + 1) if limit is not None else stream.read()
        if limit is not None and len(raw) > limit:
            raise ValueError("metadata must be at most 1 MiB")
        return json.loads(raw.decode("utf-8-sig"), object_pairs_hook=pairs,
                          parse_constant=reject, parse_float=number)
    except (ValueError, OSError) as error:
        sys.exit(f"{path}: {error}")


def _clients(region: str):
    creds = dict(
        region_name=region,
        aws_access_key_id=os.environ["MAXIMIZER_AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["MAXIMIZER_AWS_SECRET_ACCESS_KEY"],
        config=Config(retries={"max_attempts": 5, "mode": "standard"}),
    )
    s3 = boto3.client("s3", **creds)
    if os.environ.get("MAXIMIZER_UPLOAD_FUNCTION"):
        from admitted_upload import AdmittedUploads
        s3 = AdmittedUploads(s3, boto3.client("lambda", **creds),
                             os.environ["MAXIMIZER_UPLOAD_FUNCTION"], region)
    return s3, boto3.client("sqs", **creds)


def _check_ucid(ucid: str) -> None:
    # A UCID becomes part of an S3 key and of a file name. A trailing
    # "-<digits>" would be read as a part number, and "__" separates the
    # category token, so neither may appear in the UCID itself.
    if not ucid or "/" in ucid or "__" in ucid or re.fullmatch(r".+-\d+", ucid):
        sys.exit(f"{ucid!r}: a UCID may not be empty, contain '/' or '__', or end in -<digits>")


def ucid_names(category: str, ucid: str, audio_paths: list) -> list:
    """The upload names of a call's recording(s): `<category>__<UCID>.wav` for a
    one-part recording, `<category>__<UCID>-<n>.wav` (n = 1, 2, ...) for parts."""
    names = []
    for n, path in enumerate(audio_paths, start=1):
        ext = os.path.splitext(path)[1]
        if ext.lower() not in (".wav", ".mp3") or not _extension_is_accepted(path):
            sys.exit(f"{path}: a recording must be .wav or .mp3, spelled all-lower or all-upper")
        part = "" if len(audio_paths) == 1 else f"-{n}"
        names.append(f"{category}__{ucid}{part}{ext}")
    return names


def upload_ucid_call(category: str, ucid: str, metadata_path: str, audio_paths: list,
                     batch: bool = False) -> None:
    """Upload one call in UCID mode, then wait for its result `<category>/<UCID>.json`."""
    check_category(category)
    _check_ucid(ucid)
    folder = category + (BATCH_SUFFIX if batch else "")
    audio_names = ucid_names(category, ucid, audio_paths)
    upload_metadata_name = metadata_name(category, ucid, metadata_path)

    check_metadata_file(metadata_path, category, ucid, audio_names)

    region = os.environ["MAXIMIZER_AWS_REGION"]  # required: the region in your config file
    input_bucket = os.environ["MAXIMIZER_INPUT_BUCKET"]
    results_bucket = os.environ["MAXIMIZER_RESULTS_BUCKET"]
    s3, sqs = _clients(region)

    # 1) Upload every part and the one metadata file. Order does not matter: the
    #    pipeline starts the call once all of them are there.
    for path, name in zip(audio_paths, audio_names):
        key = f"{folder}/{name}"
        print(f"uploading {path} -> {input_bucket}/{key}")
        s3.upload_file(path, input_bucket, key)
    key = f"{folder}/{upload_metadata_name}"
    print(f"uploading {metadata_path} -> {input_bucket}/{key}")
    s3.upload_file(metadata_path, input_bucket, key)
    print("upload complete")

    # 2) The result is always `<category>/<UCID>.json` in the results bucket,
    #    also for a batch upload and for a multi-part call. Match the
    #    notification on that name: one message comes per call.
    result_key = f"{category}/{ucid}.json"
    _wait_for_result(s3, sqs, results_bucket, result_key)


def check_metadata_file(metadata_path: str, category: str, ucid: str, audio_names: list) -> dict:
    """Check generic JSON and call naming constraints before uploading."""
    metadata = read_json(metadata_path, 1024 * 1024)
    if not isinstance(metadata, dict):
        sys.exit(f"{metadata_path}: must be one JSON object")
    if metadata.get("ucid") != ucid:
        sys.exit(f"{metadata_path}: 'ucid' must equal the call id {ucid!r}")
    if metadata.get("audio_files") != audio_names:
        sys.exit(f"{metadata_path}: 'audio_files' must list the uploaded names exactly, "
                 f"in part order: {audio_names}")
    return metadata


def upload_org_files(org_path: str, users_path: str) -> None:
    """Upload the daily org file and users list to org/. No result is produced."""
    names = [os.path.basename(org_path), os.path.basename(users_path)]
    m_org, m_users = (ORG_NAME_RE.fullmatch(n) for n in names)
    if not (m_org and m_org.group(1) == "org" and m_users and m_users.group(1) == "users"):
        sys.exit("expected org_YYYY-MM-DD.json and users_YYYY-MM-DD.json, in that order")
    if m_org.group(2) != m_users.group(2):
        sys.exit("the org file and the users list must carry the same date")
    for path in (org_path, users_path):
        if not isinstance(read_json(path), list):
            sys.exit(f"{path}: must be a JSON array")
    region = os.environ["MAXIMIZER_AWS_REGION"]  # required: the region in your config file
    input_bucket = os.environ["MAXIMIZER_INPUT_BUCKET"]
    s3, _ = _clients(region)
    for path, name in zip((org_path, users_path), names):
        key = f"org/{name}"
        print(f"uploading {path} -> {input_bucket}/{key}")
        s3.upload_file(path, input_bucket, key)
    print("upload complete (org files produce no result file)")


def _wait_for_result(s3, sqs, results_bucket: str, result_key: str) -> None:
    queue_url = os.environ.get("MAXIMIZER_RESULTS_QUEUE_URL")
    deadline = time.time() + 15 * 60
    out_path = os.path.basename(result_key)
    if queue_url:
        print(f"waiting for the file_status notification for {result_key} on {queue_url}")
        while time.time() < deadline:
            resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
            for msg in resp.get("Messages", []):
                status = json.loads(msg["Body"])
                if status.get("results_file_name") != result_key:
                    continue  # another call's notification: leave it for its consumer
                s3.download_file(results_bucket, result_key, out_path)
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
                print(f"result ready ({status['status']} at {status['message_timestamp']}): "
                      f"{results_bucket}/{result_key} -> {out_path}")
                return
        sys.exit("timed out waiting for the notification — contact Maximizer with the UCID")
    print(f"waiting for {results_bucket}/{result_key}")
    while time.time() < deadline:
        try:
            s3.head_object(Bucket=results_bucket, Key=result_key)
        except ClientError:
            time.sleep(15)
            continue
        s3.download_file(results_bucket, result_key, out_path)
        print(f"result ready: {results_bucket}/{result_key} -> {out_path}")
        return
    sys.exit("timed out waiting for result — contact Maximizer with the UCID")


def main(category: str, audio_path: str) -> None:
    check_category(category)
    if not _extension_is_accepted(audio_path):
        sys.exit(f"{audio_path}: only .wav, .mp3 or .json, spelled all-lower or all-upper")
    filename = os.path.basename(audio_path)
    prefix = f"{category}__"
    if not filename.startswith(prefix):
        filename = prefix + filename
    key = f"{category}/{filename}"
    result_ext = os.environ.get("MAXIMIZER_RESULTS_EXT", "json").lstrip(".")
    result_key = f"{category}/{os.path.splitext(filename[len(prefix):])[0]}.{result_ext}"
    s3, sqs = _clients(os.environ["MAXIMIZER_AWS_REGION"])
    input_bucket = os.environ["MAXIMIZER_INPUT_BUCKET"]
    results_bucket = os.environ["MAXIMIZER_RESULTS_BUCKET"]
    print(f"uploading {audio_path} -> {input_bucket}/{key}")
    s3.upload_file(audio_path, input_bucket, key)
    _wait_for_result(s3, sqs, results_bucket, result_key)


USAGE = """usage:
  upload_and_fetch.py <category> <audio_file>
  upload_and_fetch.py --ucid [--batch] <category> <UCID> <metadata.json> <audio> [<audio> ...]
  upload_and_fetch.py --org <org_YYYY-MM-DD.json> <users_YYYY-MM-DD.json>"""

if __name__ == "__main__":
    args = sys.argv[1:]
    if args[:1] == ["--org"] and len(args) == 3:
        upload_org_files(args[1], args[2])
    elif args[:1] == ["--ucid"]:
        args = args[1:]
        batch = args[:1] == ["--batch"]
        if batch:
            args = args[1:]
        if len(args) < 4:
            sys.exit(USAGE)
        upload_ucid_call(args[0], args[1], args[2], args[3:], batch=batch)
    elif len(args) == 2 and not args[0].startswith("--"):
        main(args[0], args[1])
    else:
        sys.exit(USAGE)
