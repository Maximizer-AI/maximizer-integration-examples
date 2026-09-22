// Maximizer AI — AWS S3 integration example (Node.js / AWS SDK v3).
//
// Uploads a call recording to the Maximizer INPUT bucket, then retrieves the
// **client results file** from the RESULTS location.
//
// What you receive: a curated display document built only from the fields
// configured for your tenant (an allow-list); format `.json` or `.pdf`
// per your tenant config. It is NOT the internal `*.llm-results.json` dataset.
// In production the pipeline notifies you with the exact key; this example
// polls the results location under your category prefix as a simple fallback.
//
// Environment:
//   export MAXIMIZER_AWS_ACCESS_KEY_ID=...   MAXIMIZER_AWS_SECRET_ACCESS_KEY=...
//   export MAXIMIZER_AWS_REGION=us-west-2
//   export MAXIMIZER_INPUT_BUCKET=your-input-bucket
//   export MAXIMIZER_RESULTS_BUCKET=your-results-bucket
//   export MAXIMIZER_RESULTS_EXT=json        # or: pdf
//   export MAXIMIZER_RESULTS_QUEUE_URL=...   # notification queue (recommended); omit to poll the bucket
//   node upload_and_fetch.mjs claims ./call.wav
//
// Requires: npm install @aws-sdk/client-s3 @aws-sdk/client-sqs @aws-sdk/lib-storage
import { writeFile } from "node:fs/promises";
import { basename } from "node:path";
import { createReadStream } from "node:fs";
import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } from "@aws-sdk/client-sqs";
import { Upload } from "@aws-sdk/lib-storage";

// The four the pipeline's own resolver recognises. There is no "sales": a file
// uploaded under it is accepted, processed, and written where this credential
// cannot read it — a missing result rather than an upload error.
const ALLOWED = ["claims", "service", "new_policy_sales", "renew_policy_sales"];

// The bucket matches the extension case-sensitively and lists only the
// all-lower and all-upper spellings, so ".wav" and ".WAV" pass while ".Wav" is
// refused. Mirror that exactly rather than lower-casing.
const ALLOWED_EXT = [".wav", ".mp3", ".json"];
const extensionIsAccepted = (p) => {
  const m = p.match(/\.[^.\/]*$/);
  if (!m) return false;
  const ext = m[0];
  return ALLOWED_EXT.includes(ext.toLowerCase())
    && (ext === ext.toLowerCase() || ext === ext.toUpperCase());
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function keys(s3, bucket, prefix, ext) {
  const r = await s3.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix }));
  return new Set((r.Contents || []).map((o) => o.Key).filter((k) => k.endsWith(`.${ext}`)));
}

async function main(category, audioPath) {
  if (!ALLOWED.includes(category)) throw new Error(`category must be one of ${ALLOWED}`);
  if (!extensionIsAccepted(audioPath))
    throw new Error(`${audioPath}: only ${ALLOWED_EXT.join(", ")} may be uploaded, spelled all-lower or all-upper; convert or rename first`);
  const region = process.env.MAXIMIZER_AWS_REGION || "us-west-2";
  const inputBucket = process.env.MAXIMIZER_INPUT_BUCKET;
  const resultsBucket = process.env.MAXIMIZER_RESULTS_BUCKET;
  const ext = (process.env.MAXIMIZER_RESULTS_EXT || "json").replace(/^\./, "");

  const s3 = new S3Client({
    region,
    credentials: {
      accessKeyId: process.env.MAXIMIZER_AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.MAXIMIZER_AWS_SECRET_ACCESS_KEY,
    },
    maxAttempts: 5,
  });

  // The category comes from the FILE NAME, not the folder: a recording with no
  // `<category>__` token is analysed as a claim wherever it is put.
  let filename = basename(audioPath);
  if (!filename.startsWith(`${category}__`)) filename = `${category}__${filename}`;
  const key = `${category}/${filename}`;

  // 1) Upload. Encryption (SSE-KMS) and multipart are automatic.
  console.log(`uploading ${audioPath} -> s3://${inputBucket}/${key}`);
  await new Upload({ client: s3, params: { Bucket: inputBucket, Key: key, Body: createReadStream(audioPath) } }).done();
  console.log("upload complete");

  // 2) Retrieve the curated results file.
  //    Preferred: wait for the notification. For every processed file the
  //    pipeline publishes one file_status message to your environment's queue:
  //    { upload_file_name, results_file_name, status: "processed", message_timestamp }.
  //    Same access key, registered IP ranges only. Delete the message once handled.
  const deadline = Date.now() + 15 * 60 * 1000;
  const queueUrl = process.env.MAXIMIZER_RESULTS_QUEUE_URL;
  if (queueUrl) {
    const sqs = new SQSClient({ region, credentials: s3.config.credentials });
    console.log(`waiting for the file_status notification on ${queueUrl}`);
    while (Date.now() < deadline) {
      const r = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 20 }));
      for (const m of r.Messages ?? []) {
        const status = JSON.parse(m.Body);
        if (status.upload_file_name !== key) continue; // another file's notification
        await sqs.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: m.ReceiptHandle }));
        const resultKey = status.results_file_name;
        const obj = await s3.send(new GetObjectCommand({ Bucket: resultsBucket, Key: resultKey }));
        const outPath = basename(resultKey);
        await writeFile(outPath, await obj.Body.transformToByteArray());
        console.log(`result ready (${status.status} at ${status.message_timestamp}): s3://${resultsBucket}/${resultKey} -> ${outPath}`);
        return;
      }
    }
    throw new Error("timed out waiting for the notification — contact Maximizer with the upload key");
  }

  //    Fallback without a queue: poll the results location under your category prefix.
  const prefix = `${category}/`;
  const before = await keys(s3, resultsBucket, prefix, ext);
  console.log(`waiting for a new *.${ext} result under s3://${resultsBucket}/${prefix}`);

  while (Date.now() < deadline) {
    const current = await keys(s3, resultsBucket, prefix, ext);
    const fresh = [...current].filter((k) => !before.has(k)).sort();
    if (fresh.length) {
      const resultKey = fresh[fresh.length - 1];
      const obj = await s3.send(new GetObjectCommand({ Bucket: resultsBucket, Key: resultKey }));
      const body = await obj.Body.transformToByteArray();
      const outPath = basename(resultKey);
      await writeFile(outPath, body);
      console.log(`result ready: s3://${resultsBucket}/${resultKey} -> ${outPath}`);
      return;
    }
    await sleep(15000);
  }
  throw new Error("timed out waiting for result — contact Maximizer with the upload key");
}

const [category, audioPath] = process.argv.slice(2);
if (!category || !audioPath) throw new Error("usage: node upload_and_fetch.mjs <category> <audio_file>");
main(category, audioPath).catch((e) => { console.error(e.message); process.exit(1); });
