# Maximizer AI integration examples

Upload a recording, receive a completion message, and download a JSON result.
These examples show the AWS and GCP integration interfaces. Credentials,
locations, access settings, and field definitions are provided at onboarding.

Start with the [demo recording and upload steps](#demo-recording).
Reference clients: [Python](aws/python/upload_and_fetch.py),
[JavaScript](aws/javascript/upload_and_fetch.mjs), and
[Java](aws/java/UploadAndFetch.java).

## AWS model

Recordings go to the input S3 bucket. Results come from the results S3 bucket.
Completion messages arrive through SNS and SQS. The upload examples use SQS
when `MAXIMIZER_RESULTS_QUEUE_URL` is set. Otherwise they poll the exact result
object. A download is completed before the matching message is deleted.

Your categories are agreed with Maximizer at onboarding.
`<category>` uses lowercase letters, digits, and underscores, with no `__`.
The clients check this syntax without restricting categories to a fixed list.

The two-argument mode takes `<category> <audio>`. It uploads to
`<category>/<category>__<filename>`, adding the filename prefix if needed.
The result is `<category>/<stem>.json`, with that prefix and the recording
extension removed. `MAXIMIZER_RESULTS_EXT` can change this result extension.

## Call-id naming mode

`--ucid` names one call by its unique call id (UCID). It takes a category,
call id, metadata file, and recording files in part order.

| Object | Input key |
| --- | --- |
| One recording | `<category>/<category>__<UCID>.wav` |
| Multiple recordings | `<category>/<category>__<UCID>-<n>.wav` |
| Metadata | `<category>/<category>__<UCID>.<suffix>.json` |

Part numbers start at 1. Recordings may use `.wav` or `.mp3`, in all-lowercase
or all-uppercase spelling. A call id cannot contain `/` or `__`, or end in
`-<digits>`, which would be read as a part number.

`--batch` changes the input folder to `<category>_batch/`. It does not change
the filenames. Results always use `<category>/<UCID>.json`, including for
multiple parts and batch uploads.

### The metadata file

One metadata file accompanies each call. Processing starts once
the metadata and all listed recordings have arrived.

The client preserves a local filename already matching
`<category>__<UCID>.<suffix>.json`. Otherwise set `MAXIMIZER_METADATA_SUFFIX`
to **the metadata file suffix agreed at onboarding**, including its leading
dot and final `.json`. There is no default suffix. An unrecognised local
filename without that setting is refused before upload.

[Metadata format sample](samples/ucid/category_a__call001.metadata.json).
Its filename illustrates the shape; obtain the actual suffix at onboarding.
For the bundled MP3, use the [matching demo metadata](samples/audio/metadata.json)
and the [demo upload steps](#demo-recording).
The sample contains the call id, call date-time, agent and manager identity
fields, recording names, and one generic field of each value type: string,
integer, 0/1 flag, number, and null.

The full key list for your environment is agreed at onboarding and delivered
directly. It is never published here.

The clients check that metadata is one JSON object of at most 1 MiB, with no
duplicate keys, `NaN`, or `Infinity`. A UTF-8 BOM is tolerated. `ucid` must
match the call id argument. `audio_files` must list the uploaded filenames
without folders, exactly in part order. With direct-ingest credentials, a call
rejected during processing can wait for a corrected file under the same name.
[Admission-enabled uploads](#admission-enabled-uploads) keep reserved
filenames immutable; changing an admitted file requires a fresh UCID and the
metadata updates described there.

### The daily org file and users list

Upload both daily files to `org/`, using the same date:

- `org/org_YYYY-MM-DD.json` is the full organisation snapshot. Each row has
  `rep_id`, `rep_name`, `team_id`, `team`, `manager_employee_id`, `manager_name`,
  `center_id`, `center`, `array_id`, `array`, `division_id`, and `division`.
  Use stable unit identifiers. See the
  [org sample](samples/ucid/org_2026-10-04.json).
- `org/users_YYYY-MM-DD.json` lists users by `email` and `managed_unit_ids`.
  See the [users sample](samples/ucid/users_2026-10-04.json).

Both files are JSON arrays. They produce no result file or completion message.

### The completion message and result file

The SQS message contains these four fields:

```json
{
  "upload_file_name": "category_a/category_a__call001.wav",
  "results_file_name": "category_a/call001.json",
  "status": "processed",
  "message_timestamp": "2026-10-04T12:00:00+00:00"
}
```

Match on `results_file_name`. For a multiple-part call, `upload_file_name`
names just one part. `message_timestamp` is the UTC delivery time in ISO-8601
format. Delete the message after successfully handling the result.

A call with no speech has the result `{"status": "no_speech", "results": {}}`.
Its completion message still has `status: "processed"`. Do not depend on
identity fields being present in that result.

The [result sample](samples/ucid/call001.json) shows three generic parameters,
supporting quotes, a summary, a transcript as a list of timestamp and turn
items, and returned identity fields. Identify fields by key; `order` may
change. Titles and values appear in the language configured for your
environment. All sample values are placeholders.

## Demo recording

The bundled recording says: "Hello. One, two, three. Thank you."
It is synthetic speech with no names or business scenario.
The MP3 is about 4 seconds and 8 KB (mono, 16 kHz).

| Demo file | Purpose |
| --- | --- |
| [Audio recording](samples/audio/category_a__demo001.mp3) | Actual MP3 to upload. Open the link and use **Download raw file** on GitHub. |
| [Upload metadata](samples/audio/metadata.json) | Matching call id `demo001` and uploaded recording name in `audio_files`. |
| [Spoken text](samples/audio/demo001.txt) | Exact words in the recording. |
| [Illustrative result](samples/audio/demo001.json) | Example result structure with a summary, transcript, parameters, quotes, and returned identity fields. |

The result values and quote timestamps are illustrative. They are not a
captured response or an assertion about the output your configuration will
produce. The other files under `samples/ucid/` illustrate the interface format;
their `call001` recording is not bundled. Use the complete `demo001` set above
for an upload test.

### Upload the demo

1. Clone or download this repository. Run the commands from its root folder.
2. Use Python 3.10 or later and install `boto3`: `python3 -m pip install boto3`.
3. Set the [AWS environment variables](#running-the-examples), including
   `MAXIMIZER_METADATA_SUFFIX` to the full metadata ending agreed at onboarding.
   The demo's local filename, `metadata.json`, does not supply that ending.
4. Adapt the metadata to the field list agreed for your environment. Use your
   agreed category in place of `category_a` if needed. When changing the category,
   update `audio_files` to `<category>__demo001.mp3`. The client assigns that
   uploaded name; the local MP3 can keep its existing name.
5. Upload the recording and metadata:

```bash
python3 aws/python/upload_and_fetch.py --ucid category_a demo001 ./samples/audio/metadata.json ./samples/audio/category_a__demo001.mp3
```

With `category_a`, the input keys are:

```text
category_a/category_a__demo001.mp3
category_a/category_a__demo001.<suffix>.json
```

Here `.<suffix>.json` is the full ending from `MAXIMIZER_METADATA_SUFFIX`.
The client waits for `category_a/demo001.json` in the results bucket and saves
it as `demo001.json` in your current folder. It matches the completion message
by `results_file_name`. If the queue variable is omitted, it polls that exact
result key. A timeout means no matching result arrived within 15 minutes.

Use the access credentials and destination provided for your integration.
This sample exercises file upload and speech handling. Configured parameter
outputs depend on your environment.

## Running the examples

Set these variables to the values provided at onboarding:

```bash
export MAXIMIZER_AWS_ACCESS_KEY_ID='<access-key>'
export MAXIMIZER_AWS_SECRET_ACCESS_KEY='<secret-key>'
export MAXIMIZER_AWS_REGION='<region>'
export MAXIMIZER_INPUT_BUCKET='<input-bucket>'
export MAXIMIZER_RESULTS_BUCKET='<results-bucket>'
export MAXIMIZER_RESULTS_EXT=json
export MAXIMIZER_RESULTS_QUEUE_URL='<queue-url>'
# Required when the local metadata filename does not supply the agreed suffix:
export MAXIMIZER_METADATA_SUFFIX='<metadata-file-suffix-agreed-at-onboarding>'
```

The AWS Python examples require Python 3.10 or later and `boto3`. For a complete
example with included files, follow
[Upload the demo](#upload-the-demo). The CLI signatures are:

```text
python3 aws/python/upload_and_fetch.py <category> <audio>
python3 aws/python/upload_and_fetch.py --ucid <category> <ucid> <metadata.json> <audio...>
python3 aws/python/upload_and_fetch.py --ucid --batch <category> <ucid> <metadata.json> <audio...>
python3 aws/python/upload_and_fetch.py --org <org_YYYY-MM-DD.json> <users_YYYY-MM-DD.json>
python3 aws/python/upload_and_fetch.py --result-only <category> <UCID>
```

### Download a result without uploading

Use `--result-only <category> <UCID>` in the current clients to retrieve the
most recently stored `<category>/<UCID>.json` directly from S3. This works with
legacy and admission-enabled read credentials even if another consumer has
already taken the completion notification. Do not rerun a legacy upload just
to retrieve its result.

Only the access key, secret key, AWS region and results bucket settings above
are needed. Input-bucket, upload-function and queue settings are ignored in
this mode. After installing the dependencies below, run from a private output
folder, replacing `/path/to/checkout` with the repository location:

```bash
python3 /path/to/checkout/aws/python/upload_and_fetch.py --result-only category_a call001
node /path/to/checkout/aws/javascript/upload_and_fetch.mjs --result-only category_a call001
mvn -f /path/to/checkout/aws/java/pom.xml compile exec:java -Dexec.args="--result-only category_a call001"
```

The command writes `call001.json` locally and waits up to 15 minutes if the
result is not yet available. Permission and unexpected service errors fail
immediately. It does not upload, invoke processing, or read/delete queue
messages. An existing result does not prove that a later overwrite under the
same UCID has finished processing. Older client copies must be updated before
using this new option; existing upload commands keep their current behavior.

### Admission-enabled uploads

If your supplied configuration includes an upload function, also set:

```bash
export MAXIMIZER_UPLOAD_FUNCTION='<upload-function-from-your-configuration>'
```

Python, JavaScript, and Java use this setting with their normal command-line
arguments. Python needs `admitted_upload.py` beside its client. Keep the complete
`aws/javascript/` or `aws/java/` directory when using those languages, including
their helper files and dependency manifests. These credentials require admission before a file
is stored and cannot perform direct S3 writes. Uploads use temporary, reserved
transfer URLs; each completed filename is create-only. Use a new UCID for a
new test, and do not log or share transfer URLs.

A quota rejection reports `429` and a retry delay. Wait for that delay before
starting another upload. Network retries of a reserved part reuse its existing
reservation. Restarting the exact same command and unchanged file resumes a
valid reserved transfer. If the service already confirmed completion, the
command skips the file transfer.

The Python client sends each reserved part over HTTPS using the interpreter's
trust store plus the `certifi` bundle when that package is installed, so an
error naming TLS certificate verification means this Python build has no
usable CA bundle: install `certifi` (`python3 -m pip install certifi`) or the
interpreter's certificates, or point `SSL_CERT_FILE` at a CA bundle such as
the path printed by `python3 -m certifi`. Certificate verification is never
disabled, and an unverifiable transfer is reported at once instead of retried.

An incomplete reservation lasts 15 minutes. A request that never received a
transfer reservation may remain unrecoverable after an uncertain service
failure. Expired or unrecoverable incomplete requests need a fresh UCID; their
filenames stay reserved even if no file was stored. Changed or corrected file
contents also need a fresh UCID. Pass that new UCID to the command, update the
metadata's `ucid` and `audio_files` to its new uploaded filenames in part order,
and rename any UCID-named metadata file accordingly. See
[call-id naming](#call-id-naming-mode). Contact Maximizer with the UCID if
completion is unclear before replacing an uncertain upload.

The client never falls back to direct S3 on admission failure.
Temporary service failures and in-progress reservations get at most two short
retries using the same request; quota and filename-conflict rejections do not.
Results still use the configured S3 polling or SQS notification flow.
Organisation roster updates require a separately authorized upload channel;
the recording-transfer function rejects those updates.

All three clients use the same request identity for identical destination names
and file bytes. Keep the supplied function setting when using restricted
credentials. Omitting it selects the original direct-S3 mode, which requires
separately authorized direct-ingest credentials.

The following examples show the argument forms. Except for the linked org
samples, their local filenames are placeholders. Prepare metadata for the
corresponding uploaded recording names:

```bash
python3 aws/python/upload_and_fetch.py category_a ./call.wav
python3 aws/python/upload_and_fetch.py --ucid category_a call001 ./metadata.json ./call.wav
python3 aws/python/upload_and_fetch.py --ucid --batch category_a call002 ./metadata.json ./part1.wav ./part2.wav
python3 aws/python/upload_and_fetch.py --org ./samples/ucid/org_2026-10-04.json ./samples/ucid/users_2026-10-04.json
```

JavaScript and Java accept the same arguments, in the same order:

```bash
node aws/javascript/upload_and_fetch.mjs --ucid category_a call001 ./metadata.json ./call.wav
mvn -f aws/java/pom.xml compile exec:java -Dexec.args="--ucid category_a call001 ./metadata.json ./call.wav"
```

Install the JavaScript dependencies from its lockfile before running it:

```bash
npm ci --prefix aws/javascript
```

The JavaScript package includes the S3, SQS, Lambda, and multipart-upload AWS
SDK dependencies. Use Node.js 22 or later.

Java requires JDK 11 or later and Maven 3.6.3 or later. The bundled
[AWS Maven build](aws/java/pom.xml) pins the AWS SDK and Jackson dependencies;
Maven supplies the classpath when running `exec:java`. From the repository root,
compile both Java examples with:

```bash
mvn --batch-mode --no-transfer-progress -f aws/java/pom.xml compile
mvn --batch-mode --no-transfer-progress -f gcp/java/pom.xml compile
```

Compilation downloads the public dependencies. The GitHub checks build both
Java examples on JDK 11 and 17 and test the AWS upload behavior without cloud
credentials. JavaScript upload checks run on Node.js 22 and 24. Running an example
uses the environment variables supplied at onboarding.

`aws/python/consumer.py` continuously downloads results named in completion
messages. It uses the same AWS variables and optional `MAXIMIZER_DOWNLOAD_DIR`
(default `./results`).

## GCP model (Pub/Sub + GCS)

Upload the recording to the GCS input bucket under the agreed category prefix.
A Pub/Sub message announces the result. The consumers in `gcp/python/`,
`gcp/javascript/`, and `gcp/java/` download the named result from GCS.
These GCP examples expect `status: "Results ready"`, `file_name`, and
`results_file` in the message. `results_file` is the exact object key to download.

```bash
export GOOGLE_APPLICATION_CREDENTIALS='<credentials-file-path>'
export MAXIMIZER_GCP_PROJECT_ID='<project-id>'
export MAXIMIZER_GCP_SUBSCRIPTION='<subscription-id>'
export MAXIMIZER_RESULTS_BUCKET='<results-bucket>'
python3 gcp/python/consumer.py
```

Python requires `google-cloud-pubsub` and `google-cloud-storage`.
JavaScript requires `@google-cloud/pubsub` and `@google-cloud/storage`.
The [GCP Maven build](gcp/java/pom.xml) pins the Google Cloud libraries BOM
and `org.json:json`. After setting the GCP variables above, run it from the
repository root with:

```bash
mvn -f gcp/java/pom.xml compile exec:java
```

Use the access permissions supplied at onboarding. The result JSON has the
same shape across clouds.

Root files `clientExample.js` and `client_example.py` point to the maintained
examples. These are minimal references, not production clients. Add suitable
retry handling, monitoring, and duplicate-message handling for your application.
